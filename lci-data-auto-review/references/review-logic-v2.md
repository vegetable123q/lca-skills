# Review Logic v2

## 1) 物料平衡口径（新增）
- 使用 exchange 的 `commonComment/generalComment` 中的类型标签与描述进行过滤。
- 平衡只核查：
  - 左侧：`raw material input`
  - 右侧：`product output + by-product output + waste output`
- `energy input`（如电、热、燃料）先不计入平衡，仅单列记录。

## 2) 分类原则
- 先看显式标签（如 `tg_io_kind_tag`、`tg_io_uom_tag`）。
- 标签不足时，使用 flow 名称与 comment 文本关键词辅助判定。
- 若证据不完整，必须在报告中标注“证据不足”。

## 3) 单位疑似错误记录机制（新增）
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

## 4) 输出模板
- one_flow_rerun_timing.md
- one_flow_rerun_review_v2_zh.md
- one_flow_rerun_review_v2_en.md
- flow_unit_issue_log.md

每个 review 文件都应包含：
- 证据充足结论
- 证据不足结论/限制
