#ifndef PARQUET_UTILS_H
#define PARQUET_UTILS_H

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

#endif // PARQUET_UTILS_H
