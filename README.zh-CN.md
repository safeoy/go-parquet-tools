# go-parquet-tools

[English](./README.md) | [中文](./README.zh-CN.md) | [日本語](./README.ja.md)

一个用 Go 开发的 Parquet 命令行工具，目标是提供接近 Python 版 [`parquet-tools`](https://pypi.org/project/parquet-tools/) 的日常能力，并保持单二进制、易分发。

当前版本已经覆盖一组常用命令：

- `show`: 以表格查看行数据
- `head`: 查看前 N 行
- `tail`: 查看后 N 行
- `csv`: 导出 CSV
- `count`: 统计行数
- `schema`: 查看 schema
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
go run . head -n 10 ./sample.parquet
go run . tail -n 10 ./sample.parquet
go run . csv ./sample.parquet > sample.csv
go run . csv s3://bucket/path/*.parquet > sample.csv
go run . count ./sample.parquet
go run . schema ./sample.parquet
go run . inspect ./sample.parquet
```

## 输出格式

行数据命令支持：

- `show --format table|json|jsonl`
- `head --format table|json|jsonl`
- `tail --format table|json|jsonl`

结构化元数据命令支持：

- `count --format text|json`
- `schema --format text|json`
- `inspect --format text|json`

示例：

```bash
go run . show --limit 1 --format json ./sample.parquet
go run . head -n 10 --format jsonl ./sample.parquet
go run . inspect --format json ./sample.parquet
```

## 列选择与过滤

行数据相关命令支持列投影和简单过滤：

- `--columns a,b,c`
- `--where column=value`
- `--where column!=value`
- `--where column~=substr`
- `--where column^=prefix`
- `--where column$=suffix`

支持这些参数的命令：

- `show`
- `head`
- `tail`
- `csv`
- `count`

示例：

```bash
go run . show --columns group,name --where group=2 ./sample.parquet
go run . head -n 5 --columns name --format jsonl --where name~=al ./sample.parquet
go run . count --where group=2 ./sample.parquet
```

## 设计选择

- Parquet 读取库使用 `github.com/parquet-go/parquet-go`
- 默认不引入 Cobra，先用标准库 `flag` 保持依赖和启动成本更低
- `show` 和 `csv` 按顶层列输出；复杂嵌套值会被序列化为 JSON 字符串
- S3 读取当前采用一次性下载对象到内存后再解析，优先保证功能对齐

## 后续建议

- 增加 HTTP / HDFS 等更多输入源
- 增加数值比较过滤和更丰富的表达式能力
- 增加列重命名、派生列和更灵活的输出控制
- 针对超大文件做流式读取和更细粒度的内存控制
