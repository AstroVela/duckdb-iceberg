# Iceberg + DuckDB + Tantivy 用户文档

## 安装与接口

本交付提供显式快照全文索引：Iceberg 扫描可见行 → Vortex 物化文件 → Tantivy 索引 → DuckDB SQL 排名与回表。入口为 `tools/iceberg_fts.py` 的 Python API 和 CLI，使用同一连接中的原生 `iceberg_scan`、`COPY FORMAT vortex`、`vortex_fts_build/search`。不需要另一个搜索服务。

配套 `duckdb-vortex` 编译时打开 `-DVORTEX_ENABLE_INDEX_TANTIVY=ON`；在本仓库统一构建可设置 `-DICEBERG_ENABLE_TANTIVY=ON`，会启用 Vortex 和 Tantivy。扩展必须与 DuckDB 的版本、平台一致。Python 示例需安装相同版本的 `duckdb` 包；未签名的本地开发扩展需连接配置 `allow_unsigned_extensions=true`。

```python
import duckdb
from tools.iceberg_fts import build, search

con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
con.execute("LOAD '/build/vortex.duckdb_extension'")
con.execute("LOAD '/build/iceberg.duckdb_extension'")

build(con, '/warehouse/articles/metadata/v7.metadata.json', 123456789,
      '/indexes/articles-123456789', ['title', 'body'])
hits = search(con, '/indexes/articles-123456789',
              '/warehouse/articles/metadata/v7.metadata.json', 123456789,
              'rust AND search', 10, field_boosts={'title': 2.0})
for rank, file_id, row_offset, score, row in hits:
    print(rank, score, row)
```

从本仓库根目录启动 CLI：

```sh
python tools/iceberg_fts.py \
  --vortex-extension /build/vortex.duckdb_extension \
  --iceberg-extension /build/iceberg.duckdb_extension \
  build --metadata /warehouse/articles/metadata/v7.metadata.json \
  --snapshot 123456789 --index /indexes/articles-123456789 --fields title body

python tools/iceberg_fts.py \
  --vortex-extension /build/vortex.duckdb_extension \
  --iceberg-extension /build/iceberg.duckdb_extension \
  search --metadata /warehouse/articles/metadata/v7.metadata.json \
  --snapshot 123456789 --index /indexes/articles-123456789 --query '"full text"' -k 10
```

`123456789` 是占位值，请替换为 metadata 中真实 snapshot-id。父目录必须存在，索引目录必须不存在。metadata 输入必须是本地未压缩 JSON，快照必须含 schema-id。路径不能包含符号链接或 `..`；数据文件路径与访问权限由 Iceberg 扫描器处理。相对 manifest 路径需要在正确工作目录运行，生产环境建议绝对路径。

## 结果与一致性

查询支持词项、短语、字段限定和布尔表达式，使用 Tantivy 0.25 默认 analyzer 和 BM25。多个字段构成同一文档，多个数据文件共享同一语料统计。NULL 文本不产生词项，`*` 可返回所有可见行，包括 NULL 文本行。

结果按 score 降序、物化行地址升序排序，rank 从 1 开始。`row` 是快照扫描得到的所有列；`file_id/row_offset` 指向本代 `snapshot.vortex`，不是 Iceberg 原数据文件的位置，也不是业务主键。

索引绑定 table UUID、snapshot 描述、schema、字段 ID；查询必须指定期望快照。当前表追加、删除或 schema 变更后，查询新快照会拒绝旧索引。新 metadata 保留完全相同的历史快照时，可显式读取旧快照。重用同名但不同字段 ID 的列、不同表 UUID、快照过期均会被拒绝。

Iceberg position/equality deletes 在扫描阶段生效，被删除行不会进入索引、BM25 文档统计或 top-k。可见性能力取决于加载的 Iceberg reader：不支持的格式/删除类型直接报错，不退化为读取原文件。索引搜索不会再次扫描所有 Iceberg 原数据文件，而是读取已经固定的可见快照。

## 更新、故障与限制

- 每个快照独立构建到新目录。成功后应用切换索引目录；没有自动跟随 latest、增量更新或事务内索引维护。
- 一代索引保存完整快照副本，需额外磁盘和建索引时间。限制：100 万可见行，物化 Vortex ≤512 MiB，Tantivy artifacts ≤1 GiB，最多 16 个顶层 string 字段，k 为 1..10000。
- 字段名须为 ASCII 标识符；保留 `_vortex_file_id`、`_vortex_row_id`。不提供中文专用分词、高亮、嵌套字段全文索引或优化器自动改写。
- `iceberg-fts.json` 是最后原子发布的完成标记。扫描或建索引失败会删除本次创建的目录；已有目录不会覆盖。进程被强杀可能留下无完成标记目录，确认无进程使用后可手工清理。
- 不得修改或移动完成的索引目录。默认检索检查 native reference、物化源文件和索引 artifacts，损坏会失败。保留历史快照读取遵循 Iceberg 的源文件不可变约定。
- Vortex 格式 Iceberg 表仍受该仓库 reader 的 local/v2/固定 schema/非分区/primitive-column 限制。FTS 没有解除这些约束；Parquet 表沿用 Iceberg reader 自身支持范围。
- 用户连接可预先配置凭据和扩展选项。CLI 不负责 REST catalog 解析、凭据管理或自动下载扩展。调用方需从 catalog 解析得到已经提交的 metadata 路径与 snapshot ID。

## 验证

```sh
python -m unittest discover -s test/vortex -p test_fts.py -v
VORTEX_EXTENSION=/build/vortex.duckdb_extension \
ICEBERG_EXTENSION=/build/iceberg.duckdb_extension \
python -m unittest discover -s test/vortex -p test_fts.py -v
```

第一条验证 metadata 身份、字段 ID、过期快照、SQL 引号和失败清理。第二条额外执行真实扩展集成：索引同表删除前后快照，比较全文返回行与 `iceberg_scan` 的可见行，验证错误快照和源文件损坏会失败。没有扩展路径时 native 测试明确标为 skipped，不能算端到端通过。

本地验证记录：DuckDB 1.5.5 / macOS arm64 实际加载配套 Vortex 扩展，使用官方 Iceberg 1.5.5 扩展通过 equality-delete 前后快照及 position-delete 后 top-k 测试。metadata 测试 5 项通过。仓库定制的 Vortex 格式 Iceberg 测试需要本仓库 shell；已加入开启 Tantivy 的 Vortex CI，本机尚未运行该项。
