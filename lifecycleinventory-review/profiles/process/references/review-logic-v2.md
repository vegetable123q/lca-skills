# Review Logic v2.1

## 1) 物料平衡口径（延续 v2）
- 使用 exchange 的 `commonComment/generalComment` 中的类型标签与描述进行过滤。
- 平衡只核查：
  - 左侧：`raw material input`
  - 右侧：`product output + by-product output + waste output`
- `energy input`（如电、热、燃料）先不计入平衡，仅单列记录。

## 2) 2.1 基础信息核查（新增）
逐过程检查以下项目并输出结构化表格：
- 中英文名称是否齐全且可读；
- 功能单位字段是否存在；
- 系统边界表达（地理/路线相关字段）是否存在；
- 时间/地理/技术/管理元数据是否存在。

说明：2.1 阶段先做“存在性与可计算性”核查，不强行做行业语义推断。

## 3) 单位疑似错误记录机制（延续 v2）
当且仅当存在**直接证据**（语义与单位明显矛盾）时记录：
- 必填字段：
  - flow UUID
  - 当前单位
  - 建议正确单位
  - 依据
  - 置信度
- 不允许臆造：
  - 仅因“感觉不合理”不记录
  - 无法确认时写“证据不足，不下结论”

## 4) LLM 语义审核层（新增，可选）
- 默认关闭，需显式启用（`--enable-llm`）；
- 仅用于语义一致性与修订建议，不替代硬规则；
- 要求输出结构化 JSON，并标注证据不足项；
- LLM 调用失败时不影响主流程，回退到纯规则结果。

## 5) 输出模板
- one_flow_rerun_timing.md
- one_flow_rerun_review_v2_1_zh.md
- one_flow_rerun_review_v2_1_en.md
- flow_unit_issue_log.md
- review_summary_v2_1.json

每个 review 文件都应包含：
- 证据充足结论
- 证据不足结论/限制
