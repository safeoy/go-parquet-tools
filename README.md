# go-parquet-tools

一个用 Go 开发的 Parquet 命令行工具，目标是提供接近 Python 版 [`parquet-tools`](https://pypi.org/project/parquet-tools/) 的日常能力，并保持单二进制、易分发。

当前版本先覆盖最常用的三类操作：

- `show`: 以表格形式查看 Parquet 行数据
- `csv`: 将 Parquet 导出为 CSV
- `inspect`: 查看文件元数据、Schema、Row Group 和叶子列信息

并尽量对齐 Python 版 `parquet-tools` 的输入方式：

- 支持本地文件
- 支持本地 glob，例如 `data/*.parquet`
- 支持 S3 URI
- 支持 S3 glob，例如 `s3://bucket/prefix/*.parquet`

## 快速开始

```bash
go run . show ./sample.parquet
go run . show --limit 5 --width 24 ./data/*.parquet
go run . csv ./sample.parquet > sample.csv
go run . csv s3://bucket/path/*.parquet > sample.csv
go run . inspect ./sample.parquet
```

## 设计选择

- Parquet 读取库使用 `github.com/parquet-go/parquet-go`
- CLI 暂时只支持本地文件
- 默认不引入 Cobra，先用标准库 `flag` 保持依赖和启动成本更低
- `show` 和 `csv` 按顶层列输出；复杂嵌套值会被序列化为 JSON 字符串
- S3 读取当前采用一次性下载对象到内存后再解析，优先保证功能对齐

## 后续建议

- 增加 `schema`、`head`、`count` 等更细分命令
- 增加 HTTP / HDFS 等更多输入源
- 增加列投影、过滤、分页和 JSON Lines 导出
- 针对超大文件做流式读取和更细粒度的内存控制
