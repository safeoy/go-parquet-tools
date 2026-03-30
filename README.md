# go-parquet-tools

一个用 Go 开发的 Parquet 命令行工具，目标是提供接近 Python 版 [`parquet-tools`](https://pypi.org/project/parquet-tools/) 的日常能力，并保持单二进制、易分发。

当前版本先覆盖最常用的三类操作：

- `show`: 以表格形式查看 Parquet 行数据
- `csv`: 将 Parquet 导出为 CSV
- `inspect`: 查看文件元数据、Schema、Row Group 和叶子列信息

## 快速开始

```bash
go run . show ./sample.parquet
go run . show --limit 5 --width 24 ./sample.parquet
go run . csv ./sample.parquet > sample.csv
go run . inspect ./sample.parquet
```

## 设计选择

- Parquet 读取库使用 `github.com/parquet-go/parquet-go`
- CLI 暂时只支持本地文件
- 默认不引入 Cobra，先用标准库 `flag` 保持依赖和启动成本更低
- `show` 和 `csv` 目前按顶层列输出；复杂嵌套值会被序列化为 JSON 字符串

## 后续建议

- 增加 `schema`、`head`、`count` 等更细分命令
- 增加 S3 / HTTP / HDFS 等输入源
- 增加列投影、过滤、分页和 JSON Lines 导出
- 针对超大文件做流式读取和更细粒度的内存控制
