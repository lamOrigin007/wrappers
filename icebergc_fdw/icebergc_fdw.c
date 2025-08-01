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
}

