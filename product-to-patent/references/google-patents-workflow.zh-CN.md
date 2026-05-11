# Google Patents Product -> Patent Workflow

## 目标

从产品、材料或工艺目标出发，找到可用于生命周期建模的专利来源。优先选择披露可量化工艺路线的专利，而不是只描述性能、结构或电池测试结果的专利。

官方背景：

- Google Patents 支持自由文本、精确短语、assignee/inventor/date/country/status/language 等元数据限制，也支持 title/abstract/claims/CPC 字段搜索。
- Google Patents 结果页说明：结果默认按 relevance 排序，并且只展示同一 simple patent family 中排名最高的一个结果；可以下载最多 top 1000 个 CSV 结果。
- Google Patents 覆盖多个国家和专利局，但覆盖和法律状态不能当作最终法律结论。

参考：

- https://support.google.com/faqs/answer/7049475
- https://support.google.com/faqs/answer/7049588
- https://support.google.com/faqs/answer/7049585
- https://github.com/echonerve/Scrapling-Website-Scraper

## 查询设计

先把产品拆成四类词：

1. **精确产品名**：NCM811, NMC811, LiNi0.8Co0.1Mn0.1O2。
2. **功能/部件词**：cathode, positive electrode, active material, lithium ion battery。
3. **工艺词**：preparation method, coating, doping, precursor, calcination, sintering, coprecipitation。
4. **约束词**：assignee, country, before/after, status, CPC。只在需要收窄时使用。

NCM811 阴极首轮查询建议：

```text
"NCM811" cathode "preparation method"
("NCM811" OR "NMC811") ("positive electrode" OR cathode) coating
"LiNi0.8Co0.1Mn0.1O2" "preparation method"
"NCM811" precursor calcination
"nickel-rich" "811" cathode material
```

如果首轮结果偏泛，下一轮再加入：

```text
CPC=H01M4/525 "NCM811"
assignee:"Umicore" "NCM811"
country:CN "NCM811" "preparation method"
```

不要一开始把 query 写得过窄。先拿 metadata 看结果簇、CPC 和高频 assignee，再决定如何收敛。

## Metadata Helper

运行：

```bash
node product-to-patent/scripts/google-patents-metadata.mjs \
  --query '"NCM811" cathode "preparation method"' \
  --max-results 30 \
  --out-dir output/product-to-patent/ncm811-cathode/q1
```

默认 `--fetcher auto` 不先从本机直连 Google，而是使用 Jina Reader 读取 Google Patents。若 search endpoint 仍返回 Google automated-query block，NCM811 查询会自动退回 `assets/ncm811-query-plan.json` 中的 seed publications，再逐个读取公开 patent page 做详情补全。这样即使 `patents.google.com/xhr/query` 被阻断，示例流程仍能真实落盘。

输出：

- `google-patents-query.json`：公开搜索 URL 和 xhr query URL。
- `google-patents-metadata.json`：总结果数、候选列表、详情页提取信号。
- `google-patents-candidates.jsonl`：便于逐条 AI 审阅的候选行。

每条候选重点看：

- `title`, `snippet`, `assignee`, `inventor`
- `priority_date`, `filing_date`, `publication_date`, `grant_date`
- `link`, `pdf_link`
- `detail.family_members`
- `detail.cited_patents`, `detail.cited_by_patents`, `detail.similar_documents`

## 图片与流程图

针对 NCM811 阴极制造专利，下载页面图片是有意义的，但只应把它作为工艺披露的辅助证据。流程图、工艺路线图、制备流程示意图可能补全文本抽取容易丢失的单元操作顺序、包覆/掺杂路径、回流或后处理步骤；但 SEM/TEM 照片、电池结构图、性能曲线和网页装饰图片通常不能直接转成生命周期清单。

全文下载时优先使用流程图模式：

```bash
node product-to-patent/scripts/google-patents-download-fulltext.mjs \
  --metadata-file output/product-to-patent/ncm811-cathode/q1/google-patents-metadata.json \
  --out-dir output/raw \
  --delay 25 \
  --download-images \
  --image-mode flow \
  --skip-existing
```

批量 NCM811 管线也支持同样的开关：

```bash
node product-to-patent/scripts/google-patents-batch-pipeline.mjs \
  --out-dir output/raw \
  --target-count 800 \
  --max-depth 2 \
  --download-delay 6 \
  --download-images \
  --image-mode flow
```

`--image-mode flow` 只下载带 process-flow、preparation-process、manufacturing-process、schematic 或中文“流程图/工艺流程”等信号的 Google PatentImages 图像，并写入 `download-summary.json` 的 `figure_images`。Jina Reader 经常在页面顶部暴露 120 px 高的 HDA 缩略图；如果同一页还有后续 full-size figure，下载器会优先保存 full-size 图，避免流程图过于模糊。如果需要人工审阅全部专利附图，再改用 `--image-mode all`；不要把所有图默认当成可建模流程证据。

## Patent Family

Google Patents 搜索结果默认会折叠 simple patent family，因此 metadata 列表不是完整 family 清单。详情页中的 family、other versions、worldwide applications、same priority/application dates、标题/assignee 重合都需要综合判断。

审阅规则：

- Application 与 grant 版本通常算同一技术来源。
- 同族不同国家公开通常算同一技术来源，除非翻译、实施例或权利要求差异对建模有帮助。
- 优先选全文、实施例和 PDF 最清楚的版本作为 representative。
- 在 reviewed 文件中保留所有 family links，避免后续重复建模。

## 候选筛选

优先保留：

- 明确 NCM811/NMC811/LiNi0.8Co0.1Mn0.1O2 化学体系。
- 披露制备路线：共沉淀、混合、包覆、掺杂、烧结、洗涤、干燥等。
- 披露可量化参数：质量、摩尔比、浓度、pH、温度、时间、气氛、氧气、产率。
- 有实施例而不只是宽泛权利要求。
- 产品目标与生命周期模型目标一致，例如“1 kg NCM811 cathode active material”。

拒绝或降级：

- 只讲电池结构、性能测试或电解液，未披露正极材料制备。
- NCM811 只是对比样品或商品原料，而不是目标工艺产物。
- 只出现 broad nickel-rich cathode，没有 811 组成或可换算组成。
- 详情页 family 信号混乱且无法确认代表性。

## Scrapling Boundary

Scrapling 的可借鉴点是：缓存、选择器鲁棒性、动态页面抓取、并发和恢复。但本仓 skill 必须保持薄包装。本地实测中 Scrapling 的 `Fetcher`、`StealthyFetcher` 和 `DynamicFetcher` 对当前 `patents.google.com` 阻断仍返回 503，因此默认路径不依赖本机 headless/browser 指纹绕过。

如果需要以下能力，把实现放到 `tiangong-lca-cli`：

- 大规模分页抓取或长期监控。
- 浏览器/反爬/代理/动态渲染。
- Scrapling Python runtime、MCP server 或爬虫服务。
- 专利全文解析、PDF OCR、法律状态判定。

本 skill 只保留轻量 metadata helper 和 AI 审阅流程。

## Handoff To Patent -> Lifecyclemodel

当一个代表专利被标记为 `ready_for_lifecyclemodel`：

1. 打开 representative Google Patents URL 和 PDF。
2. 确认实施例中有足够工艺参数。
3. 用 `$patent-to-lifecyclemodel` 读取专利全文。
4. 在 `plan.json` 的 `source` 中记录 representative publication、title、assignee、family members 和 URL。

如果需要从 query 直接形成可复现闭环，优先使用：

```bash
node product-to-patent/scripts/product-patent-lifecyclemodel-workflow.mjs \
  --query '"NCM811" cathode "preparation method"' \
  --product-name "NCM811 cathode active material" \
  --out-dir output/product-to-patent-lifecyclemodel/ncm811-cathode \
  --download-images \
  --image-mode flow \
  --skip-existing
```

该命令保存 metadata、全文/PDF/流程图候选和 `workflow-manifest.json`，并为每个 publication 写入 `patents/<PUBLICATION>/plan-source.json`。后续编写 `patents/<PUBLICATION>/lifecyclemodel/plan.json` 时，直接复制其中的 `source` 对象，确保 company/assignee、priority/filing/publication/grant 时间、Google Patents 链接、PDF 链接和 family/citation 信息进入 `$patent-to-lifecyclemodel` 的 lifecyclemodel basic info。
