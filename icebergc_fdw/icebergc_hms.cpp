#include "postgres.h"

extern "C" {
#include "utils/palloc.h"
}

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "hive_metastore_types.h"
#include "hive_metastore_client.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Apache::Hadoop::Hive;

typedef struct PGColInfo
{
    char *name;
    char *type;
    bool nullable;
} PGColInfo;

PGColInfo *
load_iceberg_table_schema(const char *db_name,
                          const char *namespace_name,
                          const char *table_name,
                          int *out_len)
{
    std::shared_ptr<TSocket> socket(new TSocket("localhost", 9083));
    std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    ThriftHiveMetastoreClient client(protocol);

    transport->open();
    Table tbl;
    client.get_table(tbl, db_name, namespace_name, table_name);
    transport->close();

    size_t cols = tbl.sd.cols.size();
    PGColInfo *result = (PGColInfo *) palloc0(sizeof(PGColInfo) * cols);

    for (size_t i = 0; i < cols; ++i)
    {
        const FieldSchema &f = tbl.sd.cols[i];
        result[i].name = pstrdup(f.name.c_str());
        result[i].type = pstrdup(f.type.c_str());
        if (f.__isset.nullable)
            result[i].nullable = f.nullable;
        else
            result[i].nullable = true;
    }

    if (out_len)
        *out_len = cols;

    return result;
}
