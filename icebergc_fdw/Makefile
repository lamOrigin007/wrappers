EXTENSION = icebergc_fdw
MODULE_big = icebergc_fdw
OBJS = icebergc_fdw.o icebergc_hms.o parquet_utils.o

PG_CPPFLAGS += -std=c++11
SHLIB_LINK += -lthrift

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
