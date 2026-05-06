# 专利文本转 Lifecyclemodel 说明

本文说明 `patent-to-lifecyclemodel` 如何把专利或 SOP 文本转成 TianGong TIDAS `lifecyclemodel`。该 skill 是组合型包装器：LLM 只负责从源文本写一份 `plan.json`，后续工件由本地脚本和已有 builder skill 生成。

## 1. 总体转换思路

输入是一篇专利/SOP。输出不是单个过程，而是一组可连接的 ILCD `processDataSet`，再由 `lifecyclemodel-automated-builder` 组装成一个 `lifecyclemodel`，最后由 `lifecyclemodel-recursive-orchestrator` 生成发布准备包。

核心原则：

- 只通读源文本一次，抽取到 `output/<SOURCE>/plan.json`。
- 过程间连接只靠共享 `flow_key`：上游输出和下游输入使用同一个 key，脚本会分配同一个 flow UUID。
- 专利有明确数值时优先用专利；没有数值时，先使用可审计的计算或工程默认值。黑箱 item fallback 只用于关键物料、产品或操作数据缺失，导致该单元操作无法形成可辩护清单的情况。
- 电、水、纯氧和废气/固废估算必须通过 `scripts/estimate-utilities.mjs`，并把 `formula_ref`、`source_ref` 写回 exchange。

## 2. 从专利文本到 plan.json

第一步是把非结构化文本压缩为 `assets/plan.template.json` 形状：

- `source`：专利号、题名、权利人或申请人。
- `goal`：目标产品、功能单位、系统边界。默认功能单位通常为 `1 kg` 目标产品，边界通常为 cradle-to-gate。
- `geography` 与 `reference_year`：按专利公开或申请上下文填写，缺失时由 `normalize-plan.mjs` 兜底为 `GLO` 和 `unknown`。
- `flows`：所有原料、中间体、产品、共产品、废水、废气、能源流。
- `processes[]`：每个单元操作一个 process，包括 `step_id`、名称、分类、技术描述、输入、输出和 `reference_output_flow`。

每个 exchange 必须标记来源类型：

| derivation | 含义 | 额外要求 |
| --- | --- | --- |
| `Measured` | 专利直接给出了该数量。 | 不需要额外字段。 |
| `Calculated` | 由专利给出的配比、摩尔比、浓度、流量和时间等计算。 | 必须写 `calc_note`，说明公式和数字。 |
| `Estimated` | 源文本没有可直接使用的数量，使用工程默认值或辅助来源。 | 对电、水、O2、废物/排放，必须写 estimator 的 `formula_ref`，并尽量写 `source_ref`。 |

黑箱不是默认兜底。只有在已经尝试 `Measured`、`Calculated` 和 `Estimated` 后，仍缺少关键物料、产品或操作数据，导致该单元操作无法形成可辩护清单时，才应设置：

- process: `"black_box": true`
- 该 process 使用的所有 flow: `"unit": "item"`
- 该 process 的每个 exchange: `"amount": 1`
- `comment` 中说明缺少哪些关键数据，因此使用 item-based black-box

不要因为部分 exchange 缺失就把整条专利路线设成黑箱。应先拆成单元操作：能按专利数值、配比、浓度、产率、流量或工程估算建模的步骤保持 `"black_box": false`；只有关键数据缺失的具体步骤或 exchange 组使用黑箱。

## 3. plan 规范化

运行 driver 时会先执行：

```bash
node patent-to-lifecyclemodel/scripts/normalize-plan.mjs --plan output/<SOURCE>/plan.json --write --json
```

它做的事情：

- 补齐默认 `goal`、`geography`、`reference_year`、`classification`、`scale` 等字段。
- 校验每个 process 都有 `reference_output_flow`，且 outputs 里确实存在这个 flow。
- 校验 `derivation=Calculated` 时同一 exchange 必须有非空 `calc_note`。
- 校验 O2 输入只能出现在 `pure_oxygen: true` 的 process 中。
- 校验黑箱 process 的所有 flow 都是 `item`，且所有 item exchange 的 amount 都是 `1`。
- 校验 `canonical_flow_key` 的目标 flow 存在，单位一致，`conversion_factor` 为正数。

规范化后的 `plan.json` 成为后续全部工件的唯一来源。

## 4. 估算公式与参考

估算脚本是：

```bash
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode <electricity|water|oxygen|waste> --params '<json>'
```

### 4.1 电耗

适用于加热、保温、搅拌、微波等过程。返回值使用 `kWh_per_kg` 作为 electricity exchange amount。

公式：

```text
E_total = E_heatup + E_hold + E_stir
E_heatup = m_charge * Cp_eff * (T_C - 25) / 3600
E_hold = P_hold * duration_h
P_hold = k_type * (T_C - 25)
E_stir = P_stir * duration_h
kWh_per_kg = E_total / product_mass_kg
```

参数含义：

- `m_charge`：加热物料质量，kg。
- `Cp_eff`：有效比热，kJ kg^-1 K^-1。
- `T_C`：保温温度，摄氏度。
- `duration_h`：保温或反应时间，h。
- `k_type`：设备保温功率系数，kW K^-1。
- `P_stir`：搅拌功率，`batch_reactor_jacketed` 默认 `0.3 kW`，其他默认 `0`。

内置 `Cp_eff`：

| phase | Cp_eff |
| --- | ---: |
| `aqueous` | 4.18 |
| `solid` | 0.90 |
| `mixed` | 2.00 |

内置 `k_type`：

| process_type | k_type |
| --- | ---: |
| `muffle_lab_small` | 0.0012 |
| `muffle_lab_large` | 0.0020 |
| `tube_furnace` | 0.0015 |
| `rotary_kiln` | 0.0060 |
| `batch_reactor_jacketed` | 0.0004 |
| `microwave` | 0 |

微波过程不使用 `k_type`，而是：

```text
E_hold = nameplate_kw * duration_h
```

参考和校准依据：

- Perry's Chemical Engineers' Handbook 第 2 章：比热量级。
- Nabertherm / Carbolite 实验炉资料：额定功率与保温功率关系。
- Dunn et al. 2015, ANL：工业 NCM 煅烧 SEC 约 `2-7 kWh/kg`，实验室尺度通常更高。
- 宁夏中色锂电 NCM EIA 辅助校验：`133,920,000 kWh/a / 10,000 t/a = 13.392 kWh/kg`。这是厂级平均值，只做合理性检查，不替代专利工艺条件计算。

### 4.2 耗水与废水

适用于洗涤、后处理、含水涂覆等。返回 `kg_water_per_kg` 和 `kg_wastewater_per_kg`。

公式：

```text
m_wash = m_solid * wash_factor
kg_water_per_kg = m_wash / product_mass_kg
kg_wastewater_per_kg ~= m_wash / product_mass_kg
```

内置 `wash_factor`：

| wash_regime | wash_factor | 用途 |
| --- | ---: | --- |
| `coprecipitate` | 5 | 去除母液盐。 |
| `post_sinter_rinse` | 3 | 去除残余 Li 或可溶性物种。 |
| `simple_rinse` | 1.5 | 普通漂洗。 |
| `aqueous_coating_eia_aux` | 0.96 | 专利只说水系涂覆但未给水量时的 EIA 辅助值。 |

参考：

- Perry's Chemical Engineers' Handbook 第 18 章：液固萃取/洗涤量级。
- 宁夏中色锂电 EIA 辅助值：5000 t/a 涂覆线、4800 m3/a 涂覆废水，折合 `4800 / 5000 = 0.96 kg/kg`。只能用于专利已说明水系涂覆但缺量的情形。

规则：只要声明洗涤水输入，通常也要声明等量 `wastewater` 输出，除非专利明确给出蒸发、回用或其他去向。

### 4.3 纯氧消耗

只有专利明确写了纯氧气氛时才声明 O2 输入，并设置 process 的 `pure_oxygen: true`。

如果专利给了 O2 流量：

```text
V_O2 = flow_Nm3_h * duration_h
m_O2 = V_O2 * 1.429
kg_O2_per_kg = m_O2 / product_mass_kg
```

其中 `1.429 kg/Nm3` 是标准状态 O2 密度。

如果专利只说纯氧气氛、没有流量，实验室/中试默认：

```text
flow_Nm3_h = 2 * furnace_volume_m3
```

如果是工业 NCM/CAM 计划，专利写了纯氧烧结但没有 O2 流量，可显式使用：

```json
{"o2_basis":"ncm_cam_eia_auxiliary","scale":"industrial"}
```

此时使用宁夏中色锂电 EIA 辅助值：

```text
kg_O2_per_kg = 52,000 t/a industrial_O2 / 10,000 t/a NCM = 5.2 kg/kg
```

注意：EIA 值只补缺失数量；是否声明 O2 仍必须由专利文本决定。

### 4.4 废物和排放

用于专利命名了粉尘、重金属或类似排放路径，但没有数量的 NCM/CAM 煅烧或后处理步骤。

公式：

```text
factor_kg_per_kg = annual_pollutant_mass / annual_product_mass
per_batch_kg = factor_kg_per_kg * product_mass_kg
```

当前辅助锚点：

| 来源 | 因子 |
| --- | --- |
| 宁夏中色锂电 EIA Table 7-9 | PM: `1.401 t/a / 10,000 t/a = 0.0001401 kg/kg` |
| 宁夏中色锂电 EIA Table 7-9 | Ni: `0.29864 / 10,000 = 0.000029864 kg/kg` |
| 宁夏中色锂电 EIA Table 7-9 | Co: `0.238434 / 10,000 = 0.0000238434 kg/kg` |
| 宁夏中色锂电 EIA Table 7-9 | Mn: `0.270926 / 10,000 = 0.0000270926 kg/kg` |
| 广西巴莫验收监测 Table 9.3-1 | PM: `2.05 t/a / 25,000 t/a = 0.000082 kg/kg` |
| 广西巴莫验收监测 Table 9.3-1 | 重金属尘: `1.76 kg/a / 25,000,000 kg/a = 0.0000000704 kg/kg` |

这些值是辅助 CAM EIA/验收监测因子，不是专利测量值。若专利给出了废物质量、实测排放或足够做质量守恒的数据，应优先使用专利。

## 5. 水合物到无水物的 canonical flow

专利常写水合盐，但数据库可能只有无水流。做法是在 `flows` 同时声明两者，并让水合物指向无水物：

```json
"coso4_7h2o": {
  "name_en": "Cobalt sulfate heptahydrate",
  "unit": "kg",
  "canonical_flow_key": "coso4",
  "conversion_factor": 0.5513
},
"coso4": {
  "name_en": "Cobalt sulfate",
  "unit": "kg"
}
```

公式：

```text
conversion_factor = MW(anhydrous) / MW(hydrate)
                  = 154.99 / 281.10
                  ~= 0.5513
```

`materialize-from-plan.mjs` 会把 ILCD exchange 写成 canonical flow 的 UUID，并把 amount 乘以 `conversion_factor`，同时在 exchange comment 中记录转换。

## 6. 从 plan 生成 ILCD processDataSet

`materialize-from-plan.mjs` 负责把 `plan.json` 展开为多个工件：

1. 为每个 `flow_key`、process key 和 source 分配 UUID，写入 `uuids.json`。
2. 为每个 process 生成 `flows/NN-<proc_key>.json`，作为 `process-automated-builder` 的 scaffold 输入。
3. 调用 `process-automated-builder auto-build` 建立每个 process 的 scaffold run。
4. 直接从 plan 写出 `runs/<SOURCE>-combined/exports/processes/<PROC_UUID>_00.00.001.json`。
5. 从第一个 scaffold run 复制 `cache/process_from_flow_state.json` 和 `manifests/*.json` 到 `runs/<SOURCE>-combined/`，满足 lifecyclemodel builder 的本地 run 形状要求。
6. 写 `manifests/lifecyclemodel-manifest.json`，指向单一 source-specific combined run。

ILCD 数据集中的关键映射：

- `common:UUID` 来自 `uuids.procs[proc.key]`。
- 每个 exchange 的 `referenceToFlowDataSet.@refObjectId` 来自 `uuids.flows[flow_key]`。
- `quantitativeReference.referenceToReferenceFlow` 指向 reference output exchange 的 `@dataSetInternalID`。
- `dataDerivationTypeStatus` 来自 exchange 的 `derivation`。
- `calc_note`、`formula_ref`、`source_ref`、`source_quote`、`comment` 会合并进 exchange 的 `common:generalComment`。

## 7. 从 processDataSet 到 lifecyclemodel

driver 调用：

```bash
node lifecyclemodel-automated-builder/scripts/run-lifecyclemodel-automated-builder.mjs \
  build --manifest output/<SOURCE>/manifests/lifecyclemodel-manifest.json \
  --out-dir output/<SOURCE>/lifecyclemodel-run --json
```

builder 读取 `runs/<SOURCE>-combined/exports/processes/`。它通过 flow UUID 推断边：

- 某个 process 的 Output exchange 引用 flow UUID X；
- 另一个 process 的 Input exchange 也引用 flow UUID X；
- 则 X 是两者之间的连接流。

因此，边是否存在取决于 plan 中是否复用同一个 `flow_key`。若中间体在上游叫 `precursor`、下游叫 `ncm_precursor`，即使名称相似也不会形成边。

成功后关键输出：

- `lifecyclemodel-run/models/<SOURCE>-combined/tidas_bundle/lifecyclemodels/<MODEL_UUID>_<version>.json`
- `lifecyclemodel-run/models/<SOURCE>-combined/summary.json`
- `lifecyclemodel-run/models/<SOURCE>-combined/connections.json`
- `lifecyclemodel-run/reports/lifecyclemodel-auto-build-report.json`

## 8. 生成 orchestrator 请求并本地发布准备

如果 driver 传入 `--plan` 且运行 Stage 6，它会自动生成 `orchestrator-request.json`：

- 每个 process 变成一个 process node。
- 每个共享 `flow_key` 变成一个 node edge。
- 最后一个 process 连接到 root lifecyclemodel node。
- publish intent 是 `prepare_only`，不会远程写入。

随后依次运行：

```bash
lifecyclemodel-recursive-orchestrator plan
lifecyclemodel-recursive-orchestrator execute
lifecyclemodel-recursive-orchestrator publish
```

最终成功标记是：

```text
output/<SOURCE>/orchestrator-run/publish-summary.json
```

## 9. 将 output 内容入库

完成 Stage 8 并复核 `publish-summary.json` 后，可让 driver 从现有 `output/<SOURCE>/` 继续入库：

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --base output/<SOURCE> \
  --publish-only --commit --json
```

如果要从 `plan.json` 一次跑到入库：

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --publish-to-db --commit --json
```

Stage 9 不直接写远端库，而是：

1. 读取 `output/<SOURCE>/orchestrator-run/publish-bundle.json`。
2. 写 `output/<SOURCE>/publish-request.json`，使用统一 `tiangong publish run` 请求形状。
3. 调用最新 TianGong CLI 的 `tiangong publish run`。
4. 将发布结果写入 `output/<SOURCE>/publish-run/`。

省略 `--commit` 时生成 dry-run 发布请求，可用于检查入库 payload。

## 10. 验证方法

建议至少检查：

```bash
jq '{process_count, edge_count, multiplication_factors}' \
  output/<SOURCE>/lifecyclemodel-run/models/<SOURCE>-combined/summary.json

cat output/<SOURCE>/orchestrator-run/publish-summary.json
cat output/<SOURCE>/publish-run/publish-report.json
```

线性流程通常应满足 `edge_count == processes - 1`。有支路、回流或多输入中间体时，edge 数可更高。若 `edge_count: 0`，优先检查上下游是否复用了同一个 `flow_key`。

## 11. 常见错误

| 问题 | 原因 | 处理 |
| --- | --- | --- |
| `edge_count: 0` | 上下游使用了不同 flow UUID。 | 复用同一个 `flow_key`。 |
| `built_model_count: N` | 多个 process 被放在多个 run 里。 | 使用 driver 的 source-specific combined run。 |
| `reference flow missing` | `reference_output_flow` 没有对应 output exchange。 | 修正 process outputs。 |
| O2 校验失败 | 有 O2 input 但 process 没有 `pure_oxygen: true`。 | 只有专利写纯氧时才设置并保留 O2。 |
| 黑箱 process 校验失败 | `item` flow 的 amount 不是 1，或混用了 kg。 | 黑箱 process 内全部用 `item` 和 amount 1。 |
| 能耗/水耗不可复现 | LLM 直接猜数。 | 必须运行 estimator 并保存 `formula_ref`。 |
| 涂覆/掺杂产品质量守恒偏差 | 用最终复合产品分子量倒推前驱体。 | 对未涂覆基体做化学计量，涂层/掺杂单独质量守恒。 |
| 入库找不到 bundle | 尚未运行 Stage 8 publish prep。 | 先运行 `--all` 或至少完成 Stage 6，再用 `--publish-only --commit`。 |
