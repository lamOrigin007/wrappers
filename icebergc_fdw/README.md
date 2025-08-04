# icebergc_fdw

`icebergc_fdw` — пример Foreign Data Wrapper (FDW) на C, который демонстрирует доступ к
таблицам Apache Iceberg и файловому формату Parquet. Расширение использует
PGXS и может подключаться как к локальным файлам, так и к объектам в S3.

## Сборка и установка

Для сборки нужен установленный PostgreSQL с заголовочными файлами (pg_config).

```bash
make
make install
```

## Пример использования

```sql
CREATE EXTENSION IF NOT EXISTS icebergc_fdw;

CREATE SERVER iceberg_srv FOREIGN DATA WRAPPER icebergc_fdw
OPTIONS (
    catalog_uri 'thrift://hms:9083',
    warehouse   's3://my-bucket/warehouse',
    region      'us-east-1',
    aws_access_key_id     '<key>',
    aws_secret_access_key '<secret>',
    s3_endpoint 'https://s3.us-east-1.amazonaws.com'
);

CREATE FOREIGN TABLE iceberg_tbl (
    id integer,
    name text,
    price double precision,
    active boolean,
    created_at timestamp
) SERVER iceberg_srv;
```

## Опции

Опции могут указываться как на уровне сервера, так и на уровне иностранной таблицы:

- `catalog_uri` (обязательная) — URI Hive Metastore или путь к файлу.
- `warehouse` — путь к складу данных Iceberg.
- `aws_access_key_id` и `aws_secret_access_key` — учетные данные AWS.
- `region` — AWS region.
- `s3_endpoint` — необязательно задаёт явный конечный пункт S3.

## Ограничения

- только чтение `SELECT`, отсутстует `INSERT/UPDATE/DELETE`;
- ограниченная поддержка типов данных и продвижения фильтров;
- уровень ошибок и протокол логов ещё будут дорабатываться.

## Roadmap

- поддержка записи и DML-операций;
- оптимизация pushdown фильтров и проекций;
- расширение поддерживаемых типов данных;
- аутентификация по IAM/ролям и др.
