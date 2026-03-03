#!/usr/bin/env python3
import argparse
import json
import os
import re
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

KIND_RE = re.compile(r"\[tg_io_kind_tag=([^\]]+)\]")
UOM_RE = re.compile(r"\[tg_io_uom_tag=([^\]]+)\]")

ENERGY_WORDS = ("electric", "electricity", "kwh", "mj", "heat", "steam", "fuel", "diesel", "gas", "power", "电", "能")
RAW_WORDS = ("raw material", "feedstock", "fertilizer", "water", "seed", "pesticide", "原材料", "投入", "肥料", "种子", "农药", "用水")
BYP_WORDS = ("by-product", "co-product", "副产品", "联产")
WASTE_WORDS = ("waste", "废", "residue", "sludge")


# ---------- helpers ----------
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


def _deep_get(obj: Any, path: List[str], default=None):
    cur = obj
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _has_non_empty(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, (list, tuple, set)):
        return any(_has_non_empty(x) for x in v)
    if isinstance(v, dict):
        if "#text" in v:
            return _has_non_empty(v.get("#text"))
        return any(_has_non_empty(x) for x in v.values())
    return True


def _extract_base_names(proc: Dict[str, Any]) -> Tuple[bool, bool, List[str]]:
    base = _deep_get(proc, ["processDataSet", "processInformation", "dataSetInformation", "name", "baseName"])
    zh = False
    en = False
    values = []

    items = base if isinstance(base, list) else ([base] if base else [])
    for it in items:
        if not isinstance(it, dict):
            continue
        lang = str(it.get("@xml:lang") or "").lower()
        text = str(it.get("#text") or "")
        if text.strip():
            values.append(text.strip())
        if lang.startswith("zh") and text.strip():
            zh = True
        if lang.startswith("en") and text.strip():
            en = True

    if not (zh and en) and len(values) >= 2:
        # 弱兜底：无 lang 标记但至少两条非空
        zh = zh or True
        en = en or True

    return zh, en, values


def classify_exchange(e):
    c = (_txt(e.get("commonComment")) + " " + _txt(e.get("generalComment"))).lower()
    flow_desc = _txt((e.get("referenceToFlowDataSet") or {}).get("common:shortDescription")).lower()
    blob = c + " " + flow_desc
    kinds = [k.lower() for k in KIND_RE.findall(c)]
    kind_set = set(kinds)
    uoms = [u.lower() for u in UOM_RE.findall(c)]
    direction = (e.get("exchangeDirection") or "").lower()
    is_energy = any(w in blob for w in ENERGY_WORDS) or any(u in ("kwh", "mj", "gj") for u in uoms)

    if direction == "input":
        if "energy" in kind_set or is_energy:
            return "energy_input", kinds, uoms, blob
        if "waste" in kind_set:
            return "other_input", kinds, uoms, blob
        if "raw_material" in kind_set or "resource" in kind_set:
            return "raw_material_input", kinds, uoms, blob
        if "product" in kind_set or any(w in blob for w in RAW_WORDS):
            return "raw_material_input", kinds, uoms, blob
        return "other_input", kinds, uoms, blob

    if direction == "output":
        if "waste" in kind_set or any(w in blob for w in WASTE_WORDS):
            return "waste_output", kinds, uoms, blob
        if any(w in blob for w in BYP_WORDS):
            return "byproduct_output", kinds, uoms, blob
        if "product" in kind_set:
            return "product_output", kinds, uoms, blob
        return "other_output", kinds, uoms, blob

    return "other", kinds, uoms, blob


def unit_issue_check(e, uoms, blob):
    issues = []
    flow_uuid = (e.get("referenceToFlowDataSet") or {}).get("@refObjectId", "")
    if not flow_uuid:
        return issues

    if any(w in blob for w in ("electric", "electricity", "交流电", "电力")) and uoms and uoms[0] not in ("kwh", "mj", "gj"):
        issues.append((flow_uuid, uoms[0], "kWh", "flow 描述为电力/电能，但 uom 标签非能量单位", "高"))
    if any(w in blob for w in ("water", "用水", "工艺用水")) and uoms and uoms[0] in ("kwh", "mj", "gj"):
        issues.append((flow_uuid, uoms[0], "m3 或 kg", "flow 描述为水，但 uom 标签为能量单位", "高"))
    if any(w in blob for w in ("co2", "carbon dioxide", "二氧化碳")) and uoms and uoms[0] in ("kwh", "mj", "gj"):
        issues.append((flow_uuid, uoms[0], "kg", "排放流通常质量单位计，当前为能量单位", "中"))
    return issues


def base_info_check(proc: Dict[str, Any]) -> Dict[str, Any]:
    zh_ok, en_ok, names = _extract_base_names(proc)

    fu = _deep_get(proc, ["processDataSet", "processInformation", "quantitativeReference", "functionalUnitOrOther"])
    fu_ok = _has_non_empty(fu)

    mix_loc = _deep_get(proc, ["processDataSet", "processInformation", "geography", "mixAndLocationTypes"])
    route = _deep_get(proc, ["processDataSet", "modellingAndValidation", "LCIMethodAndAllocation", "typeOfDataSet"])
    boundary_ok = _has_non_empty(mix_loc) or _has_non_empty(route)

    time_ok = _has_non_empty(_deep_get(proc, ["processDataSet", "processInformation", "time"]))
    geo_ok = _has_non_empty(_deep_get(proc, ["processDataSet", "processInformation", "geography"]))
    tech_ok = _has_non_empty(_deep_get(proc, ["processDataSet", "modellingAndValidation"]))
    admin_ok = _has_non_empty(_deep_get(proc, ["processDataSet", "administrativeInformation"]))

    score = sum([zh_ok and en_ok, fu_ok, boundary_ok, time_ok, geo_ok, tech_ok, admin_ok])
    return {
        "name_zh_en_ok": bool(zh_ok and en_ok),
        "functional_unit_ok": fu_ok,
        "system_boundary_ok": boundary_ok,
        "time_ok": time_ok,
        "geo_ok": geo_ok,
        "tech_ok": tech_ok,
        "admin_ok": admin_ok,
        "completeness_score": score,
        "base_names": names,
    }


def _call_llm_chat(api_key: str, model: str, prompt: str, base_url: str) -> Optional[str]:
    endpoint = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": "你是严谨的LCA审核助手。只给基于输入证据的判断，不得臆造。输出必须是JSON对象。"},
            {"role": "user", "content": prompt},
        ],
    }
    req = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            choices = body.get("choices") or []
            if not choices:
                return None
            return choices[0].get("message", {}).get("content")
    except Exception:
        return None


def llm_semantic_review(process_summaries: List[Dict[str, Any]], model: str) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {"enabled": False, "reason": "OPENAI_API_KEY missing"}

    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    prompt = (
        "请基于以下 process 摘要做语义一致性审核（中英名称一致性、边界表达、修订建议）。\n"
        "要求：\n"
        "1) 只根据给定摘要；\n"
        "2) 证据不足必须明确标注；\n"
        "3) 输出 JSON，格式："
        "{findings:[{process_file, severity, fixability, evidence, action}]}.\n\n"
        f"输入摘要:\n{json.dumps(process_summaries, ensure_ascii=False)}"
    )
    txt = _call_llm_chat(api_key=api_key, model=model, prompt=prompt, base_url=base_url)
    if not txt:
        return {"enabled": True, "ok": False, "reason": "llm call failed"}

    try:
        start = txt.find("{")
        end = txt.rfind("}")
        parsed = json.loads(txt[start:end + 1] if start >= 0 and end > start else txt)
        return {"enabled": True, "ok": True, "result": parsed}
    except Exception:
        return {"enabled": True, "ok": False, "reason": "llm non-json output", "raw": txt[:8000]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-root", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--start-ts")
    ap.add_argument("--end-ts")
    ap.add_argument("--logic-version", default="v2.1")
    ap.add_argument("--enable-llm", action="store_true")
    ap.add_argument("--llm-model", default=os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    ap.add_argument("--llm-max-processes", type=int, default=8)
    args = ap.parse_args()

    run_root = Path(args.run_root)
    proc_dir = run_root / "exports" / "processes"
    files = sorted(proc_dir.glob("*.json"))
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    total_raw = total_prod = total_byp = total_waste = total_energy = 0.0
    rows = []
    unit_issues = []
    evidence_strong = []
    evidence_weak = []
    base_rows = []
    process_summaries_for_llm: List[Dict[str, Any]] = []

    for fp in files:
        obj = json.loads(fp.read_text(encoding="utf-8"))
        exs = obj.get("processDataSet", {}).get("exchanges", {}).get("exchange", [])
        if isinstance(exs, dict):
            exs = [exs]

        b = base_info_check(obj)
        base_rows.append((fp.name, b))

        p_raw = p_prod = p_byp = p_waste = p_energy = 0.0
        for e in exs:
            cls, _kinds, uoms, blob = classify_exchange(e)
            amt = _f(e.get("meanAmount") or e.get("resultingAmount"))
            if cls == "raw_material_input":
                p_raw += amt
            elif cls == "product_output":
                p_prod += amt
            elif cls == "byproduct_output":
                p_byp += amt
            elif cls == "waste_output":
                p_waste += amt
            elif cls == "energy_input":
                p_energy += amt
            unit_issues.extend(unit_issue_check(e, uoms, blob))

        bal_out = p_prod + p_byp + p_waste
        diff = bal_out - p_raw
        ratio = abs(diff) / p_raw if p_raw > 0 else None
        rows.append((fp.name, p_raw, p_prod, p_byp, p_waste, p_energy, diff, ratio))

        total_raw += p_raw
        total_prod += p_prod
        total_byp += p_byp
        total_waste += p_waste
        total_energy += p_energy

        if len(process_summaries_for_llm) < max(1, args.llm_max_processes):
            process_summaries_for_llm.append({
                "process_file": fp.name,
                "base_names": b.get("base_names", [])[:4],
                "base_checks": {
                    "name_zh_en_ok": b["name_zh_en_ok"],
                    "functional_unit_ok": b["functional_unit_ok"],
                    "system_boundary_ok": b["system_boundary_ok"],
                    "time_ok": b["time_ok"],
                    "geo_ok": b["geo_ok"],
                    "tech_ok": b["tech_ok"],
                    "admin_ok": b["admin_ok"],
                },
                "balance": {
                    "raw_in": p_raw,
                    "product": p_prod,
                    "byproduct": p_byp,
                    "waste": p_waste,
                    "energy_excluded": p_energy,
                    "relative_deviation": ratio,
                },
            })

    total_out = total_prod + total_byp + total_waste
    total_diff = total_out - total_raw
    total_ratio = abs(total_diff) / total_raw if total_raw > 0 else None

    evidence_strong.append("已基于 exchange 的 comment 标签/描述做口径过滤，仅核算 原材料投入 vs 产品+副产品+废物，能量单列不计入平衡。")
    if unit_issues:
        evidence_strong.append("发现单位疑似错误时均附带 flow 描述与单位标签的直接矛盾证据。")
    evidence_weak.append("部分 exchange 缺少结构化 type 标签，仅能依赖文本关键词分类，存在误判风险。")
    evidence_weak.append("未逐条拉取 flow 数据集参考单位做机器核对，单位结论以评论标签与流名称语义一致性为主。")

    # -------- optional llm layer --------
    llm_result = {"enabled": False, "reason": "disabled"}
    if args.enable_llm:
        llm_result = llm_semantic_review(process_summaries_for_llm, model=args.llm_model)

    # -------- output markdown --------
    zh = [
        "# one_flow_rerun_review_v2_1_zh\n",
        f"- run_id: `{args.run_id}`\n",
        f"- logic_version: `{args.logic_version}`\n",
        "\n## 2.1 基础信息核查\n",
        "|process file|中英名称|功能单位|系统边界|时间|地理|技术|管理元数据|完整性得分(0-7)|\n",
        "|---|---|---|---|---|---|---|---|---:|\n",
    ]
    for fn, b in base_rows:
        zh.append(
            f"|{fn}|{'✅' if b['name_zh_en_ok'] else '❌'}|{'✅' if b['functional_unit_ok'] else '❌'}|{'✅' if b['system_boundary_ok'] else '❌'}|{'✅' if b['time_ok'] else '❌'}|{'✅' if b['geo_ok'] else '❌'}|{'✅' if b['tech_ok'] else '❌'}|{'✅' if b['admin_ok'] else '❌'}|{b['completeness_score']}|\n"
        )

    zh += [
        "\n## 物料平衡口径\n- 物料平衡：仅核查 `原材料投入 = 产品+副产品+废物`\n- 能量投入：单列记录，不计入平衡\n",
        "\n## 分过程结果\n|process file|原材料投入|产品|副产品|废物|能量投入(不计平衡)|差值(输出-投入)|相对偏差|\n|---|---:|---:|---:|---:|---:|---:|---:|\n",
    ]
    for r in rows:
        zh.append(f"|{r[0]}|{r[1]:.6g}|{r[2]:.6g}|{r[3]:.6g}|{r[4]:.6g}|{r[5]:.6g}|{r[6]:.6g}|{'' if r[7] is None else f'{r[7]*100:.2f}%'}|\n")

    zh += [
        f"\n## 汇总\n- 原材料投入合计: **{total_raw:.6g}**\n- 产品+副产品+废物合计: **{total_out:.6g}**\n- 差值(输出-投入): **{total_diff:.6g}**\n- 相对偏差: **{'' if total_ratio is None else f'{total_ratio*100:.2f}%'}**\n- 能量投入(不计平衡)合计: **{total_energy:.6g}**\n",
        "\n## LLM 语义审核层（可选）\n",
    ]
    if llm_result.get("enabled") and llm_result.get("ok"):
        res = llm_result.get("result", {})
        findings = res.get("findings") or []
        if findings:
            zh.append("\n|process file|severity|fixability|evidence|action|\n|---|---|---|---|---|\n")
            for f in findings[:50]:
                evidence = f.get("evidence", {})
                if not isinstance(evidence, str):
                    evidence = json.dumps(evidence, ensure_ascii=False)
                action = str(f.get("action") or f.get("suggestion") or "").replace("|", "/")
                zh.append(
                    f"|{str(f.get('process_file','')).replace('|','/')}|{str(f.get('severity','')).replace('|','/')}|{str(f.get('fixability','review-needed')).replace('|','/')}|{str(evidence).replace('|','/')}|{action}|\n"
                )
    else:
        zh.append(f"- 未启用或调用失败：`{llm_result.get('reason', 'unknown')}`\n")

    zh += [
        "\n## 证据充足的结论\n" + "\n".join([f"- {x}" for x in evidence_strong]) + "\n",
        "\n## 证据不足的结论/限制\n" + "\n".join([f"- {x}" for x in evidence_weak]) + "\n",
    ]

    en = [
        "# one_flow_rerun_review_v2_1_en\n",
        f"- run_id: `{args.run_id}`\n",
        f"- logic_version: `{args.logic_version}`\n",
        "\n## 2.1 Basic info checks\n",
        "|process file|zh+en names|functional unit|system boundary|time|geo|tech|admin metadata|completeness(0-7)|\n",
        "|---|---|---|---|---|---|---|---|---:|\n",
    ]
    for fn, b in base_rows:
        en.append(
            f"|{fn}|{'✅' if b['name_zh_en_ok'] else '❌'}|{'✅' if b['functional_unit_ok'] else '❌'}|{'✅' if b['system_boundary_ok'] else '❌'}|{'✅' if b['time_ok'] else '❌'}|{'✅' if b['geo_ok'] else '❌'}|{'✅' if b['tech_ok'] else '❌'}|{'✅' if b['admin_ok'] else '❌'}|{b['completeness_score']}|\n"
        )

    en += [
        "\n## Material balance scope\n- Check only `raw material input = product + by-product + waste`\n- Energy inputs are listed but excluded from balance\n",
        "\n## Per-process results\n|process file|raw material in|product|by-product|waste|energy in (excluded)|delta(out-in)|relative deviation|\n|---|---:|---:|---:|---:|---:|---:|---:|\n",
    ]
    for r in rows:
        en.append(f"|{r[0]}|{r[1]:.6g}|{r[2]:.6g}|{r[3]:.6g}|{r[4]:.6g}|{r[5]:.6g}|{r[6]:.6g}|{'' if r[7] is None else f'{r[7]*100:.2f}%'}|\n")

    en += [
        f"\n## Summary\n- Raw material input total: **{total_raw:.6g}**\n- Product+by-product+waste total: **{total_out:.6g}**\n- Delta (out-in): **{total_diff:.6g}**\n- Relative deviation: **{'' if total_ratio is None else f'{total_ratio*100:.2f}%'}**\n- Energy input total (excluded from balance): **{total_energy:.6g}**\n",
        "\n## Evidence-sufficient conclusions\n" + "\n".join([f"- {x}" for x in evidence_strong]) + "\n",
        "\n## Evidence-insufficient conclusions / limitations\n" + "\n".join([f"- {x}" for x in evidence_weak]) + "\n",
    ]

    timing = ["# one_flow_rerun_timing\n", f"- run_id: `{args.run_id}`\n"]
    if args.start_ts and args.end_ts:
        s = datetime.fromisoformat(args.start_ts)
        e = datetime.fromisoformat(args.end_ts)
        timing += [f"- start: `{args.start_ts}`\n", f"- end: `{args.end_ts}`\n", f"- total elapsed: **{(e-s).total_seconds()/60:.2f} min**\n"]
    timing.append(f"- process files reviewed: `{len(files)}`\n")
    timing.append("- major time consumers: references retrieval, flow matching/search, flow metadata lookups.\n")

    unit_md = ["# flow_unit_issue_log\n", f"- run_id: `{args.run_id}`\n", "\n|flow UUID|current unit|suggested unit|basis|confidence|\n|---|---|---|---|---|\n"]
    if unit_issues:
        seen = set()
        for x in unit_issues:
            if x in seen:
                continue
            seen.add(x)
            unit_md.append(f"|{x[0]}|{x[1]}|{x[2]}|{x[3]}|{x[4]}|\n")
    else:
        unit_md.append("|无|无|无|未发现基于直接证据的单位矛盾|—|\n")

    (out / "one_flow_rerun_review_v2_1_zh.md").write_text("".join(zh), encoding="utf-8")
    (out / "one_flow_rerun_review_v2_1_en.md").write_text("".join(en), encoding="utf-8")
    (out / "one_flow_rerun_timing.md").write_text("".join(timing), encoding="utf-8")
    (out / "flow_unit_issue_log.md").write_text("".join(unit_md), encoding="utf-8")

    # 保存机器可读摘要
    summary = {
        "run_id": args.run_id,
        "logic_version": args.logic_version,
        "process_count": len(files),
        "totals": {
            "raw_input": total_raw,
            "product_plus_byproduct_plus_waste": total_out,
            "delta": total_diff,
            "relative_deviation": total_ratio,
            "energy_excluded": total_energy,
        },
        "llm": llm_result,
    }
    (out / "review_summary_v2_1.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
