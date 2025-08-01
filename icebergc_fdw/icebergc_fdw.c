#include "postgres.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "commands/defrem.h"
#include "access/htup_details.h"
#include "executor/executor.h"
#include "utils/rel.h"
#include "icebergc_hms.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(icebergc_fdw_handler);
PG_FUNCTION_INFO_V1(icebergc_fdw_validator);

static void icebergcGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void icebergcGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *icebergcGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
                                          Oid foreigntableid, ForeignPath *best_path,
                                          List *tlist, List *scan_clauses, Plan *outer_plan);
static void icebergcBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *icebergcIterateForeignScan(ForeignScanState *node);
static void icebergcEndForeignScan(ForeignScanState *node);

typedef struct IcebergcFdwOptions
{
    char *aws_access_key_id;
    char *aws_secret_access_key;
    char *region;
    char *catalog_uri;
    char *warehouse;
    char *s3_endpoint;
} IcebergcFdwOptions;

static IcebergcFdwOptions *icebergcGetOptions(Oid foreigntableid,
                                               Oid serverid);

Datum
icebergc_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *routine = makeNode(FdwRoutine);

    routine->GetForeignRelSize = icebergcGetForeignRelSize;
    routine->GetForeignPaths = icebergcGetForeignPaths;
    routine->GetForeignPlan = icebergcGetForeignPlan;
    routine->BeginForeignScan = icebergcBeginForeignScan;
    routine->IterateForeignScan = icebergcIterateForeignScan;
    routine->EndForeignScan = icebergcEndForeignScan;

    PG_RETURN_POINTER(routine);
}

Datum
icebergc_fdw_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}

static void
icebergcGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    baserel->rows = 1;
}

static void
icebergcGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    Cost startup_cost = 0;
    Cost total_cost = baserel->rows;

    add_path(baserel, (Path *) create_foreignscan_path(root, baserel,
                                                       NULL,
                                                       baserel->rows,
                                                       startup_cost,
                                                       total_cost,
                                                       NIL,
                                                       NULL,
                                                       NIL));
}

static ForeignScan *
icebergcGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                      ForeignPath *best_path, List *tlist,
                      List *scan_clauses, Plan *outer_plan)
{
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    return make_foreignscan(tlist, scan_clauses, baserel->relid,
                            NIL, NIL, NIL, NIL, outer_plan);
}

static void
icebergcBeginForeignScan(ForeignScanState *node, int eflags)
{
    Relation rel = node->ss.ss_currentRelation;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));

    IcebergcFdwOptions *opts =
        icebergcGetOptions(RelationGetRelid(rel), table->serverid);

    node->fdw_state = (void *) opts;

    /* Example: load schema from Hive Metastore */
    int col_count = 0;
    PGColInfo *cols = load_iceberg_table_schema("sample_db",
                                                "sample_namespace",
                                                "sample_table",
                                                &col_count);
    if (cols)
    {
        for (int i = 0; i < col_count; i++)
        {
            pfree(cols[i].name);
            pfree(cols[i].type);
        }
        pfree(cols);
    }
}

static TupleTableSlot *
icebergcIterateForeignScan(ForeignScanState *node)
{
    ExecClearTuple(node->ss.ss_ScanTupleSlot);
    return NULL;
}

static void
icebergcEndForeignScan(ForeignScanState *node)
{
    if (node->fdw_state)
    {
        pfree(node->fdw_state);
        node->fdw_state = NULL;
    }
}

static IcebergcFdwOptions *
icebergcGetOptions(Oid foreigntableid, Oid serverid)
{
    IcebergcFdwOptions *opts = palloc0(sizeof(IcebergcFdwOptions));
    List       *options = NIL;
    ListCell   *lc;

    ForeignTable *table = GetForeignTable(foreigntableid);
    ForeignServer *server = GetForeignServer(serverid);

    options = list_concat(options, table->options);
    options = list_concat(options, server->options);

    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "aws_access_key_id") == 0)
            opts->aws_access_key_id = pstrdup(defGetString(def));
        else if (strcmp(def->defname, "aws_secret_access_key") == 0)
            opts->aws_secret_access_key = pstrdup(defGetString(def));
        else if (strcmp(def->defname, "region") == 0)
            opts->region = pstrdup(defGetString(def));
        else if (strcmp(def->defname, "catalog_uri") == 0)
            opts->catalog_uri = pstrdup(defGetString(def));
        else if (strcmp(def->defname, "warehouse") == 0)
            opts->warehouse = pstrdup(defGetString(def));
        else if (strcmp(def->defname, "s3_endpoint") == 0)
            opts->s3_endpoint = pstrdup(defGetString(def));
    }

    return opts;
}

