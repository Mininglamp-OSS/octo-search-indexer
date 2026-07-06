# File Extractor Tool Comparison

> Author: cc-octo · Date: 2026-06-30 · Status: Draft for review
> Companion doc to `file-content-indexing-feasibility.md`（主文档 §5 抽取工具选型的深入版）
> Scope: 只调研 + 写文档，不改代码 / commit / build。
> 输入命题: 主文档 v1 钦定 Apache Tika 论证太薄；主人要求对比多方案后再定。

---

## 0. 结论（一句话 + 3 论据）

**阶段 1 用 Apache Tika Server (HTTP sidecar 模式)，阶段 2 视扫描版 PDF 占比决定要不要引入 MinerU 兜底。**

三论据：
1. **本项目搜索目标是"关键词命中 + 短片段高亮"**（IK 分词器进倒排即可），不是"结构化 RAG / 表格结构复原"—— Tika 的"纯文本抽取"输出正好匹配需求；docling/MinerU 的"高保真 markdown + 表格结构"是过度投入
2. **格式覆盖广度**：白名单 19 种可抽取格式（pdf/doc/docx/xls/xlsx/ppt/pptx/txt/csv/rtf/odt/ods/md/html/htm/json/xml/yaml/yml），Tika 一个引擎全覆盖，无需为 Office/纯文本各写一份适配层
3. **JVM 成本可控**：Tika 3.x server 单副本 -Xmx1g，镜像 ~500MB (minimal) / ~1.5GB (full)，跑在独立 Deploy 里跟 file-extractor 主容器隔离，Tika OOM 只重启 sidecar 不影响主流

**Tika 不合适的翻盘情形**（本项目未触发）：
- 需要保留表格结构复原 → docling / MinerU 胜
- 主体是扫描版中文 PDF → MinerU 胜（业务侧待实测占比）
- 追求极致轻量镜像（< 100MB）→ Go 原生胜（但格式覆盖差，运维成本转嫁开发）

---

## 1. Apache Tika 深入

### 1.1 部署三形态

| 形态 | 描述 | 推荐? | 理由 |
|---|---|---|---|
| **(a) Tika Server (HTTP)** | `apache/tika:3.3.0.0` 镜像，`--server` 模式监听 9998，`PUT /tika` 上传 bytes 得纯文本 | ✅ **推荐** | 长驻 JVM 无冷启动、支持并发、健康检查简单、K8s sidecar 天然契合、失败隔离清楚 |
| (b) Tika App CLI | 每次 fork `java -jar tika-app.jar`，短命进程 | ❌ | JVM 冷启动 1-2s，每份文件抽取成本高 3-5 倍；无并发；生产环境反模式 |
| (c) Tika Core lib 嵌入 Go | JNI / gRPC bridge 直接调 tika-core | ❌ | Go 无 JNI 生态；起 gRPC bridge 等于自己造 (a) 的翻版；无价值 |

**(a) 内部工作流**：
```
file-extractor (Go) --curl--> Tika Server (JVM sidecar, 9998)
                              |
                              +-- PDFParser (依赖 Apache PDFBox)
                              +-- OOXMLParser (docx/xlsx/pptx)
                              +-- OldExcelParser (xls, POI-HSSF)
                              +-- HWPFParser (doc, POI-HWPF)
                              +-- RTFParser (纯 Java)
                              +-- TextExtractor (txt/csv/md)
                              +-- HtmlParser (jsoup)
                              +-- ODFParser (odt/ods)
                              +-- (可选) TesseractOCRParser (OCR)
```

Sources: [apache/tika Docker Hub](https://hub.docker.com/r/apache/tika), [tika-docker GitHub](https://github.com/apache/tika-docker)

### 1.2 格式覆盖真实清单（本项目白名单 19 种）

| 扩展 | Tika 解析器 | 支持度 | 依赖 | 备注 |
|---|---|---|---|---|
| .pdf | PDFParser (Apache PDFBox 3.x) | ✅ | 无 | 中文需字体嵌入；扫描版需 OCR 见 §1.4 |
| .doc | HWPFParser (Apache POI) | ✅ | 无 | OLE2 老格式，稳定 |
| .docx | OOXMLParser (POI) | ✅ | 无 | 表格/文本混排 OK |
| .xls | OldExcelParser (POI-HSSF) | ✅ | 无 | 老 xls |
| .xlsx | OOXMLParser (POI) | ✅ | 无 | 大 xlsx 可能慢，见 §1.7 |
| .ppt | HSLFParser (POI) | ✅ | 无 | |
| .pptx | OOXMLParser (POI) | ✅ | 无 | |
| .txt | TextExtractor | ✅ | 无 | 编码检测 (juniversalchardet)，UTF-8/GBK 自动识别 |
| .csv | TextExtractor | ✅ | 无 | |
| .rtf | RTFParser | ✅ | 无 | 纯 Java 实现 |
| .odt / .ods | ODFParser | ✅ | 无 | OpenDocument |
| .md | TextExtractor (fallback) | ⚠️ | 无 | Tika 不专门解 markdown，走纯文本读取，md 语法符号会保留 |
| .html / .htm | HtmlParser (jsoup) | ✅ | 无 | 剥 tag 取纯文本 |
| .json / .xml / .yaml / .yml | TextExtractor (fallback) | ⚠️ | 无 | 无语义解析，纯文本读取（对全文搜足够） |

**结论**：19 种全覆盖，无缺口。⚠️ 标记的 5 种（md/json/xml/yaml/yml）是纯文本读取而非语义解析，对"关键词搜索"场景这是**优点不是缺点**（保留原始 token 可搜）。

Sources: [Tika Supported Formats](https://tika.apache.org/3.3.0/formats.html)

### 1.3 中文表现

**已知问题（历史）**：
- [TIKA-2080](https://issues.apache.org/jira/browse/TIKA-2080)：Tika 1.13 版本 PDFParser 对中日文有"每字符被替换为行首字符"的 bug — **已在 2.x/3.x 修复**，本项目用 3.3.0 不受影响
- 字体嵌入问题：中文 PDF 若未嵌入 CJK 字体（少见），会出现乱码 unmapped Unicode。metadata 字段 `pdf:containsNonEmbeddedFont` + `pdf:overallPercentageUnmappedUnicodeChars` 可预检
- 多层 PDF（图层叠加）文本顺序可能错乱 — 影响相邻字符组合，但**不影响关键词单个 token 命中**

**结论**：日常业务 PDF（Office 导出、财报、合同、扫描版排版正常）Tika 3.x 表现可用。**扫描版 PDF 需要开 OCR**（默认关闭）。

Sources: [TIKA-2080](https://issues.apache.org/jira/browse/TIKA-2080), [Tika Troubleshooting Wiki](https://cwiki.apache.org/confluence/display/TIKA/Troubleshooting+Tika), [Chinese PDF pain points](https://cwiki.apache.org/confluence/display/tika/ComparisonTikaAndPDFToText201811)

### 1.4 加密 / 密码保护

- `EncryptedDocumentException` 触发条件：
  - PDF owner password / user password 保护
  - Office 文档 (docx/xlsx/pptx) 打开密码
  - 加密的 OOXML zip 包
- **API 传密码**：Tika Server 支持 header `Password: <pwd>` 传解密密码；file-extractor 无法预知每个文件密码，实际使用中直接 catch 该异常 → DLQ `reason=encrypted`
- Tika 客户端 Java API：`ParseContext.set(PasswordProvider.class, ...)`（本项目 Go 侧不直接用，走 HTTP header 兜底）

Sources: [Tika PDF Password Protection](https://cwiki.apache.org/confluence/display/TIKA/PDFParser+(Apache+PDFBox))

### 1.5 扫描版 PDF (OCR)

**默认行为**：Tika **不做 OCR**，扫描版 PDF 抽取返回空串或极少文本
**启用 OCR 需要**：
- 用 `apache/tika:3.3.0.0-full` 镜像（预装 Tesseract 5.x + 语言包）
- 环境变量 `TESSERACT_OCR_PATH` 指向 tesseract 二进制
- 语言包需 `chi_sim`（简体）+ `chi_tra`（繁体）额外下载或 build 时装
- Tika `OCR_STRATEGY` 可选：`AUTO / NO_OCR / OCR_ONLY / OCR_AND_TEXT_EXTRACTION`
- 性能损耗：OCR 每页 ~1-3 秒（vs 无 OCR ~10ms/页），单机吞吐掉一个数量级

**Tika + Tesseract 中文 OCR 实测口径未找到公开 benchmark**，只有 [个人 gist 示例](https://gist.github.com/Heilum/af7dcc1fa26762ea459648e4d6a68fd1) 证明技术可行

**本项目建议**：阶段 1 用 minimal 镜像不装 OCR，扫描版 PDF 走 DLQ `reason=empty_extract`。阶段 2 根据业务侧扫描版占比决定是否切 full 镜像或走 MinerU 兜底（§4.4）。

Sources: [Tika OCR docs](https://cwiki.apache.org/confluence/display/TIKA/TikaOCR), [TIKA-2844](https://issues.apache.org/jira/browse/TIKA-2844)

### 1.6 性能基准

**公开 benchmark 稀缺**（社区共识：Tika 性能强依赖文档类型，通用数字意义不大）

**社区经验值**（未找到 2025 权威数字，标为**待实测**）：
- 纯文本 (txt/md/json)：~500-1000 docs/s（IO 主导）
- Office 简单文档 (docx <100KB)：~50-100 docs/s
- PDF 10MB 无 OCR：~1-5 docs/s
- PDF 扫描版 + OCR：~0.1-0.5 pages/s

**本项目预估**：
- 假设 QPS 峰值 100 docs/s（远超实际 IM 文件上传峰值）
- Tika sidecar 单副本足够；如需扩容起 3 副本 = 300 docs/s buffer
- **等 test 环境搭起后跑真实业务 PDF 拿实测数据**

Sources: [CogStack tika-service benchmark notes](https://github.com/CogStack/tika-service/blob/master/README.md)

### 1.7 JVM 依赖成本

| 指标 | 值 | 备注 |
|---|---|---|
| 镜像大小 (minimal) | ~500-700MB | Tika 3.3.0.0 无 OCR / GDAL |
| 镜像大小 (full) | ~1.5-2GB | 含 Tesseract + 部分语言包 |
| 启动时间 | ~10-15s | JVM 冷启动 + Tika 初始化 |
| 内存 baseline | ~300MB | 空载 JVM |
| 推荐 -Xmx | 1g | 处理 100MB 上限文件；小文件场景可降到 512m |
| 并发模型 | thread pool (默认 CPU × 2) | HTTP handler 每请求 1 线程 |
| **重要 flag** | `--spawnChild` (2.x+ 默认开) | 抽取跑在 fork child JVM，父进程崩溃自恢复 |

**OOM 兜底**：Tika 2.x+ 走 pipes-async 模式，单文件 OOM 只 kill child，不带崩 server。生产实践加健康检查清 `/tmp` 孤儿文件。

Sources: [Apache mailing list on OOM best practices](https://lists.apache.org/thread/nsxz14twh7hq20dvcs2pvb311sho6gqk), [tongwang/tika-server tuning](https://hub.docker.com/r/tongwang/tika-server)

### 1.8 稳定性坑（3 个真实生产事故 pattern）

1. **MP4Parser OOM** — 视频文件抽 metadata 触发老 MP4 库 OOM，Tika team 已知计划替换。**缓解**：白名单不含 mp4（本项目白名单已排除媒体格式，天然免疫）
2. **PDFBox 特定 PDF 无限循环** — 有些 malformed PDF 让 PDFBox 陷入循环，靠 Tika `--spawnChild` + tikaServerTaskTimeoutMillis 超时兜底
3. **`/tmp` 磁盘塞满** — 大批量抽取产生临时文件，Tika 不总清理彻底；生产环境加 healthcheck 定期清 `/tmp` 或用 tmpfs

Sources: [Apache Tika mailing list](https://lists.apache.org/thread/nsxz14twh7hq20dvcs2pvb311sho6gqk)

---

## 2. 候选方案对比

### 2.1 Docling (IBM Research)

**介绍**：IBM DS4SD 2024 年开源的文档转换框架，用 DocLayNet (layout) + TableFormer (table) 深度学习模型输出高保真 markdown。2025 年 10 月推出 Granite-Docling-258M VLM 模型，支持 Chinese 但**标注为 experimental**。2026 年初捐给 Linux Foundation。

**部署形态**：Python venv，pip install docling（安装体积 ~1GB+ 含模型权重），无 GPU 也能跑（CPU 慢），跟 file-extractor 集成走 HTTP / subprocess CLI

**集成方式**：`docling <input> --output md` CLI，或起 Python HTTP 服务 wrapper

**中文实测**：Granite-Docling 中文标为 experimental，IBM 官方说明"not yet enterprise-validated"。Procycons 2025 benchmark 只测英文文档

**性能**：1 页 ~6.28s，50 页 ~65s，**线性增长**（vs LlamaParse 6s 常数）；PDF 表格 94%+ 结构准确

**适用性打分（本项目）**：3/5 — 输出质量高但**过度**投入（本项目搜索不需要 markdown 结构）+ 中文实验性 + Python 生态外挂

Sources: [PDF Extraction Benchmark 2025](https://procycons.com/en/blogs/pdf-data-extraction-benchmark/), [Docling GitHub](https://github.com/docling-project/docling), [IBM Granite-Docling](https://www.ibm.com/new/announcements/granite-docling-end-to-end-document-conversion), [Docling arxiv paper](https://arxiv.org/html/2501.17887v1)

---

### 2.2 unstructured.io

**介绍**：Python 库 + 商用 API，主打 RAG 数据管道，支持 64+ 文件类型 + 50+ source/destination connectors。定位是"一站式 ETL"而非"最精"

**部署形态**：`pip install unstructured[all-docs]` 装全格式约 500MB；商用 Serverless API 起步 $1/1000 pages

**集成方式**：Python HTTP wrapper，或调 Serverless API（云依赖）

**中文实测**：文档未见专门中文 benchmark，Nov 2025 Unstructured 自家 benchmark 声称"format-agnostic table 0.844"，未拆分语言

**性能**：Procycons 2025 评价"简单 table 100%，复杂 table 75%"，速度中等，需 post-processing

**适用性打分（本项目）**：2/5 — 定位是"pipeline"，本项目已经用 Kafka 做 pipeline，不需要再套一层；商用 API 有云依赖不符本项目自建原则

Sources: [Unstructured benchmark blog](https://unstructured.io/blog/unstructured-leads-in-document-parsing-quality-benchmarks-tell-the-full-story), [Procycons benchmark](https://procycons.com/en/blogs/pdf-data-extraction-benchmark/), [Chonkie/Docling/Unstructured comparison](https://www.thinkdeeply.ai/post/a-comparative-analysis-of-data-pre-processing-frameworks-for-retrieval-augmented-generation-chonkie)

---

### 2.3 MinerU (Shanghai AI Lab, OpenDataLab)

**介绍**：上海人工智能实验室 OpenDataLab 团队开源的复杂 PDF 提取工具，含 PaddleOCR + StructEqTable + 自研 layout 模型。MinerU 2.5 (2025 年底) 是 1.2B VLM 参数 SOTA，在 OmniDocBench 上 90.67 分，超过 Gemini 2.5 Pro

**部署形态**：Python + GPU 强烈推荐（A100 上 2.12 pages/s；CPU 慢 10 倍以上），pip install magic-pdf（安装体积 ~2GB 含模型），镜像 build 后 ~5-8GB

**集成方式**：CLI (`magic-pdf convert`)，或起 HTTP wrapper；file-extractor 走 subprocess

**中文实测**：**中文 CJK 场景王者**。Ocean-OCR benchmark 中文 F1 0.965、edit distance 0.082。社区共识"没有其他工具能在中文/日文/韩文 layout detection 上打过 MinerU"。表格结构复原用 TableMaster+StructEqTable，公式转 LaTeX。**限制**：初期只测中英，2025 扩展到 109 语种但非中英仍待验证；复杂表格行列偶有错

**性能**：A100 GPU 上 2.12 pages/s；无 GPU 环境 ~0.1-0.5 pages/s（推理密集）

**适用性打分（本项目）**：
- 阶段 1: **1/5** — GPU 硬依赖 + 镜像巨大 + 输出高保真结构不匹配本项目"关键词命中"需求
- 阶段 2（如果扫描版中文 PDF 占比 > 20%）: **5/5** — 中文扫描版无出其右

Sources: [MinerU GitHub](https://github.com/opendatalab/MinerU), [MinerU2.5 arxiv](https://ar5iv.labs.arxiv.org/html/2409.18839), [MinerU 2.5 benchmark blog](https://neurohive.io/en/state-of-the-art/mineru2-5-open-source-1-2b-model-for-pdf-parsing-outperforms-gemini-2-5-pro-on-benchmarks/), [12 Open-Source PDF Parsing Tools Comparison](https://liduos.com/en/posts/ai-develope-tools-series-2-open-source-doucment-parsing/)

---

### 2.4 Go 原生组合

**介绍**：Go 生态各格式独立库拼装。核心库：
- `pdfcpu/pdfcpu` — PDF 操作，抽文本弱
- `dslipak/pdf` / `ledongthuc/pdf` — 纯 Go PDF 文本抽取（中文支持一般）
- `unidoc/unipdf` — 商业级 PDF 库（AGPL 或商业 license，本项目慎用）
- `qax-os/excelize` — xlsx，很成熟
- `richardlehane/mscfb` + `unidoc/unioffice` — Office 老格式
- `blackfriday` — markdown 解析
- `microcosm-cc/bluemonday` — HTML 剥 tag

**部署形态**：编译进 file-extractor Go 二进制，无外部依赖，镜像 ~50MB

**集成方式**：直接函数调用

**中文实测**：`ledongthuc/pdf` 对中文 PDF 有已知问题（字体表映射不完整）。Office 系列成熟度尚可

**适用性打分（本项目）**：2/5
- 优点：镜像最小、无 JVM/Python、部署最快
- 缺点：**格式覆盖靠拼装** — 19 种白名单需要至少 6-8 个库，每种维护 + 升级路径独立；PDF 中文支持不如 Tika PDFBox 成熟；老格式 (.doc/.xls/.ppt) 靠 unidoc 商业库或功能受限的 open source
- 决策：**除非**"必须无 JVM/Python 依赖"的极端约束，否则不划算

Sources: [ledongthuc/pdf](https://github.com/ledongthuc/pdf), [pdfcpu](https://github.com/pdfcpu/pdfcpu), [unidoc/unipdf license](https://github.com/unidoc/unipdf)

---

### 2.5 LibreOffice headless

**介绍**：`soffice --headless --convert-to txt <file>`，粗暴但覆盖 Office 全家 + PDF 出 → txt

**部署形态**：`libreoffice` 镜像 ~1.5GB，subprocess 调 `soffice` CLI

**集成方式**：subprocess exec

**中文实测**：LibreOffice 中文 PDF 转 txt 效果尚可，但 layout 复杂时顺序错乱严重

**适用性打分（本项目）**：2/5 — 覆盖全但**每次 fork soffice 进程冷启动 2-5s**、并发差、稳定性坑多（LibreOffice server mode 也非常规选择）；不如 Tika 长驻 JVM

Sources: [LibreOffice CLI docs](https://help.libreoffice.org/latest/en-US/text/shared/guide/start_parameters.html)

---

### 2.6 云 API 方案（一句话）

- **AWS Textract / Azure Document Intelligence / 阿里云文档智能**：托管、按 page 计费、中文 OK，**本项目不用** — 数据出境合规 + 云 API 依赖，与"自建 IM"定位冲突

---

## 3. 决策矩阵

五维打分（1=差 / 5=优）：

| 候选 | 覆盖率 | 中文效果 | 部署难度 (低=好) | 资源占用 (低=好) | 维护成本 (低=好) | **综合** | **适用阶段** |
|---|---|---|---|---|---|---|---|
| **Apache Tika Server** | 5 | 4 | 4 | 3 | 4 | **20/25** | ✅ 阶段 1 |
| Docling | 4 | 3 (实验) | 3 | 2 | 3 | 15/25 | 备选 |
| unstructured.io | 5 | 3 | 3 | 2 | 3 | 16/25 | 不推荐 |
| MinerU | 3 (PDF 强，其他弱) | 5 | 2 | 1 (GPU) | 2 | 13/25 | ✅ 阶段 2 兜底 |
| Go 原生 | 3 | 2 | 5 | 5 | 2 | 17/25 | 特殊约束下备选 |
| LibreOffice headless | 4 | 3 | 3 | 2 | 3 | 15/25 | 不推荐 |

**分支决策**：
- **默认路径**：Tika Server (§1)
- **如果扫描版中文 PDF 占比 > 20%**：Tika (主) + MinerU (扫描版兜底，double-parse)
- **如果部署环境完全无 JVM 允许**（不太可能）：Go 原生组合，接受覆盖率下降
- **如果业务扩到 RAG / 表格结构复原需求**：切 Docling

---

## 4. MVP 阶段建议

### 4.1 阶段 1 (本次上线，~2 周)
- **主抽取引擎**：Apache Tika Server 3.3.0.0 minimal 镜像
- **OCR**：不开
- **形态**：file-extractor Go 主容器 + Tika JVM sidecar（同 Pod，emptyDir 共享临时盘）
- **扫描版 PDF**：Tika 返空 → DLQ `reason=empty_extract`
- **加密 PDF/Office**：`EncryptedDocumentException` → DLQ `reason=encrypted`（Java 侧异常 → HTTP 500 → Go 侧 catch reason 字符串写 DLQ）

### 4.2 阶段 2 决策点
上线 2 周后统计：
- DLQ `reason=empty_extract` 占 type=8 总量比例
- DLQ `reason=encrypted` 占比
- 用户搜索中"预期能命中但没命中"投诉率

**分支决策**：
| 情况 | 动作 |
|---|---|
| `empty_extract` < 10% | 阶段 2 不做 OCR，成本收益比不合适 |
| `empty_extract` 10-30% | 切 Tika full 镜像开 Tesseract OCR（成本 +内存 500MB + 抽取变慢） |
| `empty_extract` > 30%，且业务侧确认多为中文扫描版 | 上 MinerU 作为**兜底 pipeline**：Tika 抽出为空的文件转发 MinerU 补抽 |
| encrypted > 5% | 与 PRD 讨论"能否引导用户去密码 / 声明政策不搜密码文件" |

### 4.3 双方案共存必要性
- **不需要 Tika + docling 共存**：功能重叠，docling 不填 Tika 的坑
- **可能需要 Tika + MinerU 共存**：MinerU 专攻扫描版中文，Tika 处理原生 PDF/Office，非重叠 — 值得做但**阶段 1 不做**，等实证数据

### 4.4 切换成本
- 阶段 1 → 阶段 2 只加 sidecar，不改 file-extractor 主流逻辑（多加一个 fallback client 调 MinerU 补抽）
- mapping / reader 完全不动
- Kafka topic / consumer group 完全不动
- 只加 Deploy，切换是**可撤销**的（关掉 MinerU sidecar 回到纯 Tika 无副作用）

---

## 5. 未决问题（要主人 / Max 拍板）

1. **prod type=8 里扫描版 PDF 占比未知** — 需 Max 从跳板抽 100 个真实业务 PDF，肉眼判"文本原生 vs 扫描"占比，决定阶段 2 是否上 MinerU
2. **阶段 1 是否直接用 Tika full 镜像预留 OCR 能力** — full 镜像大 1GB 但开 OCR 只改 config；建议**用 minimal**，需要 OCR 时切镜像即可（K8s image rollout 分钟级）
3. **file-extractor + Tika sidecar 部署单元** — 建议同 Pod 双容器（sidecar pattern，共享 emptyDir）；替代方案独立 Deploy + Service（Tika 服务化，可复用给其他消费方）—— 阶段 1 单 Pod 更省事

---

## 6. 给 Max 的回执清单

- (a) **文档路径**：`~/Project/Mininglamp-OSS/octo-search-indexer/docs/file-extractor-tool-comparison.md`
- (b) **最终推荐**：Apache Tika Server (HTTP sidecar 模式) 作为阶段 1 主引擎；阶段 2 视扫描版 PDF 实际占比引入 MinerU 兜底
- (c) **3 个关键论据**：
  1. 本项目搜索场景 = "关键词命中 + 短片段高亮"，Tika 纯文本输出正好匹配（docling/MinerU 的 markdown/表格复原是过度投入）
  2. 19 种白名单格式一个引擎全覆盖，Go 原生组合需 6-8 库拼装维护成本失控
  3. JVM 成本可控 — Tika 3.x sidecar minimal 500MB + Xmx 1g，独立容器隔离，OOM 只崩 sidecar
- (d) **翻盘理由**：**没有发现 Tika 在本项目场景不合适的强证据**。MinerU 中文场景更强但 GPU 硬依赖 + 输出高保真结构不匹配需求；docling 中文标 experimental 不能用；unstructured 定位是 ETL pipeline，本项目已有 Kafka pipeline 不需要；Go 原生格式覆盖差不划算

---

## 附录 A: 参考资料汇总

**Apache Tika**:
- [apache/tika Docker Hub](https://hub.docker.com/r/apache/tika)
- [tika-docker GitHub](https://github.com/apache/tika-docker)
- [Tika Troubleshooting Wiki](https://cwiki.apache.org/confluence/display/TIKA/Troubleshooting+Tika)
- [Tika PDF vs pdftotext Comparison](https://cwiki.apache.org/confluence/display/tika/ComparisonTikaAndPDFToText201811)
- [TIKA-2080 Chinese PDF bug (fixed)](https://issues.apache.org/jira/browse/TIKA-2080)
- [TIKA-2844 OCR strategy](https://issues.apache.org/jira/browse/TIKA-2844)
- [Apache Tika OOM best practices mailing list](https://lists.apache.org/thread/nsxz14twh7hq20dvcs2pvb311sho6gqk)
- [CogStack tika-service benchmark notes](https://github.com/CogStack/tika-service/blob/master/README.md)
- [Chinese OCR with Tika + Tesseract gist](https://gist.github.com/Heilum/af7dcc1fa26762ea459648e4d6a68fd1)

**Docling**:
- [Docling GitHub](https://github.com/docling-project/docling)
- [Docling arxiv paper](https://arxiv.org/html/2501.17887v1)
- [IBM Granite-Docling announcement](https://www.ibm.com/new/announcements/granite-docling-end-to-end-document-conversion)
- [Granite-Docling 258M InfoQ](https://www.infoq.com/news/2025/10/granite-docling-ibm/)

**MinerU**:
- [MinerU GitHub](https://github.com/opendatalab/MinerU)
- [MinerU arxiv paper](https://ar5iv.labs.arxiv.org/html/2409.18839)
- [MinerU 2.5 benchmark](https://neurohive.io/en/state-of-the-art/mineru2-5-open-source-1-2b-model-for-pdf-parsing-outperforms-gemini-2-5-pro-on-benchmarks/)

**综合对比**:
- [PDF Data Extraction Benchmark 2025 - Procycons](https://procycons.com/en/blogs/pdf-data-extraction-benchmark/)
- [12 Open-Source PDF Parsing Tools Evaluated - Meursault](https://liduos.com/en/posts/ai-develope-tools-series-2-open-source-doucment-parsing/)
- [Unstructured benchmark blog](https://unstructured.io/blog/unstructured-leads-in-document-parsing-quality-benchmarks-tell-the-full-story)
- [Chonkie/Docling/Unstructured RAG comparison](https://www.thinkdeeply.ai/post/a-comparative-analysis-of-data-pre-processing-frameworks-for-retrieval-augmented-generation-chonkie)
- [Best Open-Source PDF-to-Markdown Tools 2026](https://themenonlab.blog/blog/best-open-source-pdf-to-markdown-tools-2026)
