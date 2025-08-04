#ifndef PARQUET_UTILS_H
#define PARQUET_UTILS_H

#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <variant>

struct ColumnValue {
    enum Type { BOOL, INT32, INT64, FLOAT, DOUBLE, STRING, TIMESTAMP, DECIMAL } type;
    std::variant<bool, int32_t, int64_t, float, double, std::string> value;
};

struct RowTuple {
    std::vector<ColumnValue> columns;
};

std::vector<uint8_t> download_s3_to_buffer(const std::string &bucket,
                                           const std::string &key);

std::vector<uint8_t> download_hdfs_to_buffer(const std::string &path);

std::vector<RowTuple> parse_parquet_buffer(const uint8_t *data,
                                           size_t length,
                                           size_t max_rows);

extern "C" {
#endif

#include <stdbool.h>

typedef struct ParquetReader ParquetReader;

ParquetReader *parquet_reader_open(const char *path);
bool parquet_reader_next(ParquetReader *reader, char **values, int ncols);
void parquet_reader_close(ParquetReader *reader);

#ifdef __cplusplus
}
#endif

#endif // PARQUET_UTILS_H

