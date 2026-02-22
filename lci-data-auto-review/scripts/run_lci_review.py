#!/usr/bin/env python3
import argparse, json, re
from pathlib import Path
from datetime import datetime

KIND_RE = re.compile(r"\[tg_io_kind_tag=([^\]]+)\]")
UOM_RE = re.compile(r"\[tg_io_uom_tag=([^\]]+)\]")

ENERGY_WORDS = ("electric", "electricity", "kwh", "mj", "heat", "steam", "fuel", "diesel", "gas", "power", "电", "能")
RAW_WORDS = ("raw material", "feedstock", "fertilizer", "water", "seed", "pesticide", "原材料", "投入", "肥料", "种子", "农药", "用水")
BYP_WORDS = ("by-product", "co-product", "副产品", "联产")
WASTE_WORDS = ("waste", "废", "residue", "sludge")


def _txt(v):
    if isinstance(v, list):
        return " ".join(str(i.get("#text", "")) for i in v if isinstance(i, dict))
    if isinstance(v, dict):
        return str(v.get("#text", ""))
    return str(v or "")


def _f(x):
    try:
        return float(x)
    except Exception:
        return 0.0


def classify_exchange(e):
    c = (_txt(e.get("commonComment")) + " " + _txt(e.get("generalComment"))).lower()
    flow_desc = _txt((e.get("referenceToFlowDataSet") or {}).get("common:shortDescription")).lower()
    blob = c + " " + flow_desc
    kinds = [k.lower() for k in KIND_RE.findall(c)]
    uoms = [u.lower() for u in UOM_RE.findall(c)]
    direction = (e.get("exchangeDirection") or "").lower()
    is_energy = any(w in blob for w in ENERGY_WORDS) or any(u in ("kwh", "mj", "gj") for u in uoms)

    if direction == "input":
        if is_energy:
            return "energy_input", kinds, uoms, blob
        if "waste" in kinds:
            return "other_input", kinds, uoms, blob
        if "product" in kinds or any(w in blob for w in RAW_WORDS):
            return "raw_material_input", kinds, uoms, blob
        return "other_input", kinds, uoms, blob

    if direction == "output":
        if "waste" in kinds or any(w in blob for w in WASTE_WORDS):
            return "waste_output", kinds, uoms, blob
        if any(w in blob for w in BYP_WORDS):
            return "byproduct_output", kinds, uoms, blob
        if "product" in kinds:
            return "product_output", kinds, uoms, blob
        return "other_output", kinds, uoms, blob

    return "other", kinds, uoms, blob


def unit_issue_check(e, kinds, uoms, blob):
    issues = []
    flow_uuid = (e.get("referenceToFlowDataSet") or {}).get("@refObjectId", "")
    if not flow_uuid:
        return issues
    # Evidence-based only: strong lexical contradiction
    if any(w in blob for w in ("electric", "electricity", "交流电", "电力")) and uoms and uoms[0] not in ("kwh", "mj", "gj"):
        issues.append((flow_uuid, uoms[0], "kWh", "flow 描述为电力/电能，但 uom 标签非能量单位", "高"))
    if any(w in blob for w in ("water", "用水", "工艺用水")) and uoms and uoms[0] in ("kwh", "mj", "gj"):
        issues.append((flow_uuid, uoms[0], "m3 或 kg", "flow 描述为水，但 uom 标签为能量单位", "高"))
    if any(w in blob for w in ("co2", "carbon dioxide", "二氧化碳")) and uoms and uoms[0] in ("kwh", "mj", "gj"):
        issues.append((flow_uuid, uoms[0], "kg", "排放流通常质量单位计，当前为能量单位", "中"))
    return issues


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-root", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--start-ts")
    ap.add_argument("--end-ts")
    args = ap.parse_args()

    run_root = Path(args.run_root)
    proc_dir = run_root / "exports" / "processes"
    files = sorted(proc_dir.glob("*.json"))
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)

    total_raw = total_prod = total_byp = total_waste = total_energy = 0.0
    rows = []
    unit_issues = []
    evidence_strong = []
    evidence_weak = []

    for fp in files:
        obj = json.loads(fp.read_text(encoding="utf-8"))
        exs = obj.get("processDataSet", {}).get("exchanges", {}).get("exchange", [])
        if isinstance(exs, dict):
            exs = [exs]
        p_raw = p_prod = p_byp = p_waste = p_energy = 0.0
        for e in exs:
            cls, kinds, uoms, blob = classify_exchange(e)
            amt = _f(e.get("meanAmount") or e.get("resultingAmount"))
            if cls == "raw_material_input": p_raw += amt
            elif cls == "product_output": p_prod += amt
            elif cls == "byproduct_output": p_byp += amt
            elif cls == "waste_output": p_waste += amt
            elif cls == "energy_input": p_energy += amt
            unit_issues.extend(unit_issue_check(e, kinds, uoms, blob))
        bal_out = p_prod + p_byp + p_waste
        diff = bal_out - p_raw
        ratio = abs(diff) / p_raw if p_raw > 0 else None
        rows.append((fp.name, p_raw, p_prod, p_byp, p_waste, p_energy, diff, ratio))
        total_raw += p_raw; total_prod += p_prod; total_byp += p_byp; total_waste += p_waste; total_energy += p_energy

    total_out = total_prod + total_byp + total_waste
    total_diff = total_out - total_raw
    total_ratio = abs(total_diff) / total_raw if total_raw > 0 else None

    # evidence statements
    evidence_strong.append("已基于 exchange 的 comment 标签/描述做口径过滤，仅核算 原材料投入 vs 产品+副产品+废物，能量单列不计入平衡。")
    if unit_issues:
        evidence_strong.append("发现单位疑似错误时均附带 flow 描述与单位标签的直接矛盾证据。")
    evidence_weak.append("部分 exchange 缺少结构化 type 标签，仅能依赖文本关键词分类，存在误判风险。")
    evidence_weak.append("未逐条拉取 flow 数据集的参考单位进行机器核对，单位结论以评论标签与流名称语义一致性为主。")

    zh = [f"# one_flow_rerun_review_v2_zh\n", f"- run_id: `{args.run_id}`\n", "\n## 口径\n- 物料平衡：仅核查 `原材料投入 = 产品+副产品+废物`\n- 能量投入：单列记录，不计入平衡\n",
          "\n## 分过程结果\n|process file|原材料投入|产品|副产品|废物|能量投入(不计平衡)|差值(输出-投入)|相对偏差|\n|---|---:|---:|---:|---:|---:|---:|---:|\n"]
    for r in rows:
        zh.append(f"|{r[0]}|{r[1]:.6g}|{r[2]:.6g}|{r[3]:.6g}|{r[4]:.6g}|{r[5]:.6g}|{r[6]:.6g}|{'' if r[7] is None else f'{r[7]*100:.2f}%'}|\n")
    zh += [f"\n## 汇总\n- 原材料投入合计: **{total_raw:.6g}**\n- 产品+副产品+废物合计: **{total_out:.6g}**\n- 差值(输出-投入): **{total_diff:.6g}**\n- 相对偏差: **{'' if total_ratio is None else f'{total_ratio*100:.2f}%'}**\n- 能量投入(不计平衡)合计: **{total_energy:.6g}**\n",
           "\n## 证据充足的结论\n" + "\n".join([f"- {x}" for x in evidence_strong]) + "\n",
           "\n## 证据不足的结论/限制\n" + "\n".join([f"- {x}" for x in evidence_weak]) + "\n"]

    en = [f"# one_flow_rerun_review_v2_en\n", f"- run_id: `{args.run_id}`\n", "\n## Scope\n- Material balance checks only: `raw material input = product + by-product + waste`\n- Energy inputs are tracked but excluded from balance\n",
          "\n## Per-process results\n|process file|raw material in|product|by-product|waste|energy in (excluded)|delta(out-in)|relative deviation|\n|---|---:|---:|---:|---:|---:|---:|---:|\n"]
    for r in rows:
        en.append(f"|{r[0]}|{r[1]:.6g}|{r[2]:.6g}|{r[3]:.6g}|{r[4]:.6g}|{r[5]:.6g}|{r[6]:.6g}|{'' if r[7] is None else f'{r[7]*100:.2f}%'}|\n")
    en += [f"\n## Summary\n- Raw material input total: **{total_raw:.6g}**\n- Product+by-product+waste total: **{total_out:.6g}**\n- Delta (out-in): **{total_diff:.6g}**\n- Relative deviation: **{'' if total_ratio is None else f'{total_ratio*100:.2f}%'}**\n- Energy input total (excluded from balance): **{total_energy:.6g}**\n",
           "\n## Evidence-sufficient conclusions\n" + "\n".join([f"- {x}" for x in evidence_strong]) + "\n",
           "\n## Evidence-insufficient conclusions / limitations\n" + "\n".join([f"- {x}" for x in evidence_weak]) + "\n"]

    timing = ["# one_flow_rerun_timing\n", f"- run_id: `{args.run_id}`\n"]
    if args.start_ts and args.end_ts:
        s = datetime.fromisoformat(args.start_ts)
        e = datetime.fromisoformat(args.end_ts)
        timing += [f"- start: `{args.start_ts}`\n", f"- end: `{args.end_ts}`\n", f"- total elapsed: **{(e-s).total_seconds()/60:.2f} min**\n"]
    timing.append(f"- process files reviewed: `{len(files)}`\n")
    timing.append("- major time consumers (from run behavior/log): references retrieval, flow matching/search, flow metadata lookups.\n")

    unit_md = ["# flow_unit_issue_log\n", f"- run_id: `{args.run_id}`\n", "\n|flow UUID|current unit|suggested unit|basis|confidence|\n|---|---|---|---|---|\n"]
    if unit_issues:
        seen=set()
        for x in unit_issues:
            if x in seen: continue
            seen.add(x)
            unit_md.append(f"|{x[0]}|{x[1]}|{x[2]}|{x[3]}|{x[4]}|\n")
    else:
        unit_md.append("|无|无|无|未发现基于直接证据的单位矛盾|—|\n")

    (out / "one_flow_rerun_review_v2_zh.md").write_text("".join(zh), encoding="utf-8")
    (out / "one_flow_rerun_review_v2_en.md").write_text("".join(en), encoding="utf-8")
    (out / "one_flow_rerun_timing.md").write_text("".join(timing), encoding="utf-8")
    (out / "flow_unit_issue_log.md").write_text("".join(unit_md), encoding="utf-8")

if __name__ == "__main__":
    main()
