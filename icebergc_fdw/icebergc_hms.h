#ifndef ICEBERGC_HMS_H
#define ICEBERGC_HMS_H

#include "postgres.h"

typedef struct PGColInfo
{
    char *name;
    char *type;
    bool nullable;
} PGColInfo;

PGColInfo *load_iceberg_table_schema(const char *db_name,
                                     const char *namespace_name,
                                     const char *table_name,
                                     int *out_len);

#endif /* ICEBERGC_HMS_H */
