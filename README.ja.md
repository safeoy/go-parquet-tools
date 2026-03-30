# go-parquet-tools

[English](./README.md) | [中文](./README.zh-CN.md) | [日本語](./README.ja.md)

Go で実装された Parquet 向けのコマンドラインツールです。Python 版 [`parquet-tools`](https://pypi.org/project/parquet-tools/) の日常的な操作感に近づけつつ、単一バイナリで簡単に配布できることを目標にしています。

現在のバージョンでは、実用的なコマンド群を提供しています。

- `show`: 行データを表形式で表示
- `head`: 先頭 N 行を表示
- `tail`: 末尾 N 行を表示
- `csv`: 行データを CSV として出力
- `count`: 行数を集計
- `schema`: スキーマを表示
- `inspect`: ファイルメタデータ、スキーマ、Row Group、リーフ列の詳細を表示

入力方式は Python 版にできるだけ近づけています。

- ローカルファイル
- `data/*.parquet` のようなローカル glob
- S3 URI
- `s3://bucket/prefix/*.parquet` のような S3 glob

## Quick Start

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

## Output Formats

行指向コマンドでは以下をサポートします。

- `show --format table|json|jsonl`
- `head --format table|json|jsonl`
- `tail --format table|json|jsonl`

構造化メタデータ系コマンドでは以下をサポートします。

- `count --format text|json`
- `schema --format text|json`
- `inspect --format text|json`

例:

```bash
go run . show --limit 1 --format json ./sample.parquet
go run . head -n 10 --format jsonl ./sample.parquet
go run . inspect --format json ./sample.parquet
```

## Column Selection And Filtering

行指向コマンドでは、列選択とシンプルな文字列フィルタを利用できます。

- `--columns a,b,c`
- `--where column=value`
- `--where column!=value`
- `--where column~=substr`
- `--where column^=prefix`
- `--where column$=suffix`

対応コマンド:

- `show`
- `head`
- `tail`
- `csv`
- `count`

例:

```bash
go run . show --columns group,name --where group=2 ./sample.parquet
go run . head -n 5 --columns name --format jsonl --where name~=al ./sample.parquet
go run . count --where group=2 ./sample.parquet
```

## Design Choices

- Parquet のデコードには `github.com/parquet-go/parquet-go` を使用
- 依存関係と起動コストを抑えるため、現時点では Cobra を使わず標準ライブラリの `flag` を利用
- `show` と `csv` はトップレベル列を対象に動作し、複雑なネスト値は現在 JSON 文字列として直列化
- S3 読み込みは現時点ではオブジェクト全体をメモリに読み込んでから解析し、まずは機能互換性を優先

## Next Steps

- HTTP や HDFS など、追加の入力ソースをサポート
- 数値比較フィルタや、より豊富な式サポートを追加
- 列名変更、派生列、より柔軟な出力制御を追加
- 大きなファイル向けにストリーミング読み込みとメモリ制御を改善
