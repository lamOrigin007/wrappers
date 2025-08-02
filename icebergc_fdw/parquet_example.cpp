#include "parquet_utils.h"
#include <iostream>

int main() {
    std::string path = "s3://my-bucket/data.parquet";
    size_t n = 5;

    std::vector<uint8_t> data;
    if (path.rfind("s3://", 0) == 0) {
        auto pos = path.find('/', 5);
        std::string bucket = path.substr(5, pos - 5);
        std::string key = path.substr(pos + 1);
        data = download_s3_to_buffer(bucket, key);
    } else if (path.rfind("hdfs://", 0) == 0) {
        data = download_hdfs_to_buffer(path);
    }

    auto rows = parse_parquet_buffer(data.data(), data.size(), n);

    for (const auto &row : rows) {
        std::cout << "Row:";
        for (const auto &cell : row.columns) {
            std::visit([](auto &&val) { std::cout << ' ' << val; }, cell.value);
        }
        std::cout << "\n";
    }

    return 0;
}
