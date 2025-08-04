#include "parquet_utils.h"

#include <aws/common/init.h>
#include <aws/s3/s3.h>
#include <hdfs/hdfs.h>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <stdexcept>
#include <algorithm>
#include <fstream>
#include <limits>
#include <cstring>

extern "C" {
#include "postgres.h"
#include "utils/palloc.h"
}

std::vector<uint8_t> download_s3_to_buffer(const std::string &bucket,
                                           const std::string &key) {
    aws_common_library_init(aws_default_allocator());

    struct aws_allocator *alloc = aws_default_allocator();
    struct aws_s3_client_config cfg;
    AWS_ZERO_STRUCT(cfg);
    struct aws_s3_client *client = aws_s3_client_new(alloc, &cfg);

    struct aws_byte_buf body_buf;
    aws_byte_buf_init(&body_buf, alloc, 16 * 1024);

    struct aws_s3_get_object_request_options opt;
    AWS_ZERO_STRUCT(opt);
    opt.bucket_name = aws_byte_cursor_from_c_str(bucket.c_str());
    opt.object_key = aws_byte_cursor_from_c_str(key.c_str());
    opt.out_body = &body_buf;

    if (aws_s3_client_make_get_object_request(client, &opt)) {
        aws_s3_client_release(client);
        throw std::runtime_error("failed to download object from s3");
    }
    aws_s3_client_release(client);

    std::vector<uint8_t> result(body_buf.buffer, body_buf.buffer + body_buf.len);
    aws_byte_buf_clean_up(&body_buf);
    return result;
}

std::vector<uint8_t> download_hdfs_to_buffer(const std::string &path) {
    hdfsFS fs = hdfsConnect("default", 0);
    if (!fs)
        throw std::runtime_error("failed to connect to hdfs");
    hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
    if (!file)
        throw std::runtime_error("failed to open hdfs file");
    tOffset size = hdfsAvailable(fs, file);
    std::vector<uint8_t> buf(size);
    tSize n = hdfsRead(fs, file, buf.data(), size);
    if (n != size)
        throw std::runtime_error("hdfs read error");
    hdfsCloseFile(fs, file);
    hdfsDisconnect(fs);
    return buf;
}

std::vector<RowTuple> parse_parquet_buffer(const uint8_t *data,
                                           size_t length,
                                           size_t max_rows) {
    auto buffer = std::make_shared<arrow::Buffer>(data, length);
    auto pf = parquet::ParquetFileReader::Open(
        std::make_shared<arrow::io::BufferReader>(buffer));
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(pf),
                                     &arrow_reader);

    std::shared_ptr<arrow::Table> table;
    arrow_reader->ReadTable(&table);

    std::vector<RowTuple> rows;
    int num_cols = table->num_columns();
    int64_t rows_to_read = std::min<int64_t>(table->num_rows(), max_rows);

    for (int64_t r = 0; r < rows_to_read; ++r) {
        RowTuple row;
        row.columns.reserve(num_cols);
        for (int c = 0; c < num_cols; ++c) {
            auto arr = table->column(c)->chunk(0);
            ColumnValue cell{};
            switch (arr->type_id()) {
            case arrow::Type::BOOL:
                cell.type = ColumnValue::BOOL;
                cell.value = std::static_pointer_cast<arrow::BooleanArray>(arr)->Value(r);
                break;
            case arrow::Type::INT32:
                cell.type = ColumnValue::INT32;
                cell.value = std::static_pointer_cast<arrow::Int32Array>(arr)->Value(r);
                break;
            case arrow::Type::INT64:
                cell.type = ColumnValue::INT64;
                cell.value = std::static_pointer_cast<arrow::Int64Array>(arr)->Value(r);
                break;
            case arrow::Type::FLOAT:
                cell.type = ColumnValue::FLOAT;
                cell.value = std::static_pointer_cast<arrow::FloatArray>(arr)->Value(r);
                break;
            case arrow::Type::DOUBLE:
                cell.type = ColumnValue::DOUBLE;
                cell.value = std::static_pointer_cast<arrow::DoubleArray>(arr)->Value(r);
                break;
            case arrow::Type::STRING:
            case arrow::Type::BINARY:
                cell.type = ColumnValue::STRING;
                cell.value = std::static_pointer_cast<arrow::BinaryArray>(arr)->GetString(r);
                break;
            case arrow::Type::TIMESTAMP:
                cell.type = ColumnValue::TIMESTAMP;
                cell.value = std::static_pointer_cast<arrow::TimestampArray>(arr)->Value(r);
                break;
            case arrow::Type::DECIMAL128:
                cell.type = ColumnValue::DECIMAL;
                cell.value = std::static_pointer_cast<arrow::Decimal128Array>(arr)
                                 ->FormatValue(r);
                break;
            default:
                cell.type = ColumnValue::STRING;
                cell.value = "";
            }
            row.columns.push_back(std::move(cell));
        }
        rows.push_back(std::move(row));
    }

    return rows;
}

struct ParquetReader {
    std::vector<RowTuple> rows;
    size_t index;
};

static std::string column_value_to_string(const ColumnValue &cell) {
    switch (cell.type) {
    case ColumnValue::BOOL:
        return std::get<bool>(cell.value) ? "t" : "f";
    case ColumnValue::INT32:
        return std::to_string(std::get<int32_t>(cell.value));
    case ColumnValue::INT64:
        return std::to_string(std::get<int64_t>(cell.value));
    case ColumnValue::FLOAT:
        return std::to_string(std::get<float>(cell.value));
    case ColumnValue::DOUBLE:
        return std::to_string(std::get<double>(cell.value));
    case ColumnValue::STRING:
    case ColumnValue::DECIMAL:
        return std::get<std::string>(cell.value);
    case ColumnValue::TIMESTAMP:
        return std::to_string(std::get<int64_t>(cell.value));
    default:
        return "";
    }
}

extern "C" ParquetReader *parquet_reader_open(const char *path) {
    std::vector<uint8_t> data;
    std::string spath(path);
    if (spath.rfind("s3://", 0) == 0) {
        auto pos = spath.find('/', 5);
        std::string bucket = spath.substr(5, pos - 5);
        std::string key = spath.substr(pos + 1);
        data = download_s3_to_buffer(bucket, key);
    } else if (spath.rfind("hdfs://", 0) == 0) {
        data = download_hdfs_to_buffer(spath);
    } else {
        std::ifstream file(path, std::ios::binary);
        if (!file)
            return NULL;
        data = std::vector<uint8_t>(std::istreambuf_iterator<char>(file),
                                    std::istreambuf_iterator<char>());
    }

    auto rows = parse_parquet_buffer(data.data(), data.size(),
                                     std::numeric_limits<size_t>::max());

    ParquetReader *reader = new ParquetReader();
    reader->rows = std::move(rows);
    reader->index = 0;
    return reader;
}

extern "C" bool parquet_reader_next(ParquetReader *reader, char **values, int ncols) {
    if (!reader || reader->index >= reader->rows.size())
        return false;

    const RowTuple &row = reader->rows[reader->index++];
    int cols = std::min<int>(ncols, row.columns.size());
    for (int i = 0; i < cols; ++i) {
        std::string s = column_value_to_string(row.columns[i]);
        values[i] = pstrdup(s.c_str());
    }
    for (int i = cols; i < ncols; ++i)
        values[i] = NULL;
    return true;
}

extern "C" void parquet_reader_close(ParquetReader *reader) {
    delete reader;
}

