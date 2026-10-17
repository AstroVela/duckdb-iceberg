# Iceberg 全文检索设计

## 交付结构

| 仓库 | 职责 |
| --- | --- |
| AstroVela/vortex | FullTextIndex、TantivyProvider、多文件 BM25、native SQL bind/execute/cache/take |
| AstroVela/duckdb-vortex | index-tantivy feature、CMake 开关、provider 注册、扩展加载入口 |
| AstroVela/duckdb-iceberg | 本文对应的快照适配工具、身份验证、故障清理、集成测试 |

采用显式不可变快照索引。Python 负责流程与 Iceberg 身份，所有数据扫描、Vortex 编解码、全文建索引及搜索在原生 DuckDB/Rust/Tantivy 中执行。Python 不逐行索引正文，也不自行实现 Iceberg 删除规则。

## 为什么物化快照

直接把各个数据文件的 top-k 合并会混用不同 BM25 统计；在排名后再做 delete 过滤会漏掉应递补的结果。改为先执行固定 snapshot 的完整 Iceberg scan，取得可见行并物化，随后只构建一个 Tantivy index。position/equality deletes、字段 ID 和历史 schema 由现有 Iceberg reader 处理，删除行不贡献词频或文档数。

这个选择换取明确的语义和已有 reader 的兼容性，代价是额外磁盘和全量扫描。它不是原 Iceberg 数据文件的 row-address sidecar，也不透明加速任意 SQL；未来直接 IndexSource 适配可在同样的快照和可见性契约下替换物化阶段。

## 发布和身份

1. 验证 metadata JSON 的大小、table UUID、指定 snapshot 和 schema-id，解析被索引字段的 ID/type。
2. 独占创建新 generation 目录。调用 `iceberg_scan(..., snapshot_from_id := ID)` 并 `COPY` 为 `snapshot.vortex`。
3. 最多扫描 1000001 行，多出的哨兵行会触发后端百万行上限，禁止静默截断。检查物化文件 ≤512 MiB。
4. 调用 `vortex_fts_build`，由后端执行索引提交、checksum 清单、重开验证、native reference 原子发布。
5. 再校验原 metadata 字节没有变化，保存其副本。最后 fsync 并原子发布 `iceberg-fts.json`。

完成标记包括 format version、table UUID、完整 snapshot/schema、选定字段 ID/name、元数据 SHA-256、native reference SHA-256、行数、物化地址空间说明。没有完成标记的目录不可查询。发生异常只清理本次独占目录，不能影响已有索引。

查询重新解析调用方指定的 metadata/snapshot，严格比较 snapshot/schema/字段 ID 身份和 native reference hash。原表提交新的 snapshot 不会让旧索引冒充最新内容；明确选定的旧 snapshot 可以继续使用。native SQL strict 模式继续验证物化源和 artifacts。API 不把 metadata 文件路径当作唯一表身份。

## 资源、故障和运行假设

本地索引路径固定并拒绝 symlink；source snapshot 不在每次搜索重新下载，遵循 Iceberg 不可变文件契约。元数据中的远端数据路径沿用连接的文件系统/凭据策略。输入 metadata 本身仅支持未压缩本地 JSON。

构建耗时和空间与整个可见快照相关，最多 100 万行、512 MiB Vortex、1 GiB 索引；字段上限 16，query/k 受 native 层限制。所有应用发布/切换目录的引用管理由调用者负责；清理必须等活跃查询结束。强杀后可能有未发布目录，重试使用新目录或清理孤儿目录。并发不能共享同一个 DuckDB connection 执行操作。

没有 native `iceberg_fts_*` SQL table function；对 Iceberg 的便捷入口是可导入的 Python API/CLI。DuckDB 原生 `vortex_fts_*` SQL 是实际执行接口。文档和示例明确区分这两层，避免将便捷工具误解为 catalog optimizer 集成。

## 验证策略

metadata 单测覆盖错误 snapshot、table/field identity、已存在目标、异常清理、SQL 转义和路径约束。真实扩展测试对同表 append 和 equality-delete 后快照分别建索引，`*` 查询回表结果必须与 `iceberg_scan` 完全一致；错误 snapshot 与物化文件损坏必须失败。底层 provider 测试验证多文件相同分数与确定性平分、RowFilter 在 top-k 前生效、boost、checksum 和跨快照拒绝；DuckDB 回归覆盖 prepared ownership、external access 和 source validation。

测试同时支持 Python 动态扩展和 `python test/vortex/test_fts.py --duckdb /path/to/shell`。CI 的 Vortex ON 构建启用 Tantivy，执行全部场景；Vortex OFF 构建不加载或执行全文功能。position-delete 测试从本机 REST fixture 提交真实 DELETE，再检查 top-1 不会返回已删除行。
