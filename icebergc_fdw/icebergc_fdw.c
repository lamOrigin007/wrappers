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
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "nodes/primnodes.h"
#include "icebergc_hms.h"
#include "parquet_utils.h"

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

typedef enum
{
    ICEBERG_FILTER_OP,
    ICEBERG_FILTER_BETWEEN
} IcebergFilterKind;

typedef struct IcebergFilter
{
    IcebergFilterKind kind;
    char *column;
    char *op;      /* used when kind == ICEBERG_FILTER_OP */
    char *val1;
    char *val2;    /* used for BETWEEN */
} IcebergFilter;

typedef struct IcebergScanState
{
    IcebergcFdwOptions *opts;
    List *filters;      /* list of IcebergFilter* */
    List *columns;      /* list of column names */
    ParquetReader *reader;    /* current parquet reader */
    AttInMetadata *attinmeta; /* attribute input metadata */
    char **values;            /* row buffer */
} IcebergScanState;

static List *extract_filters(Relation rel, List *quals);
static List *extract_projection(Relation rel, List *tlist);
static char *datum_to_cstring(Datum d, Oid typeoid);
static bool list_member_str(List *list, const char *str);

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
                            NIL, NIL, tlist, NIL, outer_plan);
}

static char *
datum_to_cstring(Datum d, Oid typeoid)
{
    Oid typoutput;
    bool typisvarlena;
    getTypeOutputInfo(typeoid, &typoutput, &typisvarlena);
    return OidOutputFunctionCall(typoutput, d);
}

static IcebergFilter *
make_op_filter(Relation rel, OpExpr *op)
{
    Node *larg = linitial(op->args);
    Node *rarg = lsecond(op->args);
    Var *var = NULL;
    Const *cst = NULL;

    if (IsA(larg, Var) && IsA(rarg, Const))
    {
        var = (Var *) larg;
        cst = (Const *) rarg;
    }
    else if (IsA(rarg, Var) && IsA(larg, Const))
    {
        var = (Var *) rarg;
        cst = (Const *) larg;
    }
    if (!var || !cst)
        return NULL;

    IcebergFilter *f = palloc0(sizeof(IcebergFilter));
    f->kind = ICEBERG_FILTER_OP;
    f->column = pstrdup(get_attname(RelationGetRelid(rel), var->varattno, false));
    f->op = pstrdup(get_opname(op->opno));
    f->val1 = datum_to_cstring(cst->constvalue, cst->consttype);
    return f;
}

static bool
is_between_clause(Relation rel, Expr *e1, Expr *e2, IcebergFilter **out)
{
    if (!IsA(e1, OpExpr) || !IsA(e2, OpExpr))
        return false;
    IcebergFilter *f1 = make_op_filter(rel, (OpExpr *) e1);
    IcebergFilter *f2 = make_op_filter(rel, (OpExpr *) e2);
    if (!f1 || !f2)
        return false;
    if (strcmp(f1->column, f2->column) != 0)
        return false;
    if (!((strcmp(f1->op, ">=") == 0 && strcmp(f2->op, "<=") == 0) ||
          (strcmp(f1->op, "<=") == 0 && strcmp(f2->op, ">=") == 0)))
        return false;
    IcebergFilter *f = palloc0(sizeof(IcebergFilter));
    f->kind = ICEBERG_FILTER_BETWEEN;
    f->column = f1->column;
    if (strcmp(f1->op, ">=") == 0)
    {
        f->val1 = f1->val1;
        f->val2 = f2->val1;
        pfree(f2->column);
    }
    else
    {
        f->val1 = f2->val1;
        f->val2 = f1->val1;
        pfree(f1->column);
    }
    pfree(f1->op); pfree(f1); pfree(f2->op); pfree(f2);
    *out = f;
    return true;
}

static void
append_filter(Relation rel, Expr *expr, List **out)
{
    if (IsA(expr, OpExpr))
    {
        IcebergFilter *f = make_op_filter(rel, (OpExpr *) expr);
        if (f)
            *out = lappend(*out, f);
    }
    else if (IsA(expr, BoolExpr))
    {
        BoolExpr *b = (BoolExpr *) expr;
        if (b->boolop == AND && list_length(b->args) == 2)
        {
            IcebergFilter *bf;
            if (is_between_clause(rel, linitial(b->args), lsecond(b->args), &bf))
            {
                *out = lappend(*out, bf);
                return;
            }
        }
        ListCell *lc;
        foreach(lc, b->args)
            append_filter(rel, (Expr *) lfirst(lc), out);
    }
}

static List *
extract_filters(Relation rel, List *quals)
{
    List *result = NIL;
    ListCell *lc;
    foreach(lc, quals)
        append_filter(rel, (Expr *) lfirst(lc), &result);
    return result;
}

static bool
list_member_str(List *list, const char *str)
{
    ListCell *lc;
    foreach(lc, list)
        if (strcmp((const char *) lfirst(lc), str) == 0)
            return true;
    return false;
}

static List *
extract_projection(Relation rel, List *tlist)
{
    List *cols = NIL;
    ListCell *lc;
    foreach(lc, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);
        if (tle->resjunk)
            continue;
        if (IsA(tle->expr, Var))
        {
            Var *var = (Var *) tle->expr;
            char *name = get_attname(RelationGetRelid(rel), var->varattno, false);
            if (!list_member_str(cols, name))
                cols = lappend(cols, pstrdup(name));
        }
    }
    return cols;
}

static void
icebergcBeginForeignScan(ForeignScanState *node, int eflags)
{
    Relation rel = node->ss.ss_currentRelation;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));

    IcebergScanState *state = palloc0(sizeof(IcebergScanState));
    state->opts = icebergcGetOptions(RelationGetRelid(rel), table->serverid);

    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    state->filters = extract_filters(rel, fsplan->scan.plan.qual);
    state->columns = extract_projection(rel, fsplan->fs_targetlist);

    TupleDesc tupdesc = RelationGetDescr(rel);
    state->attinmeta = TupleDescGetAttInMetadata(tupdesc);
    state->values = (char **) palloc0(tupdesc->natts * sizeof(char *));
    if (state->opts && state->opts->catalog_uri)
        state->reader = parquet_reader_open(state->opts->catalog_uri);
    if (!state->reader)
        ereport(ERROR,
                (errmsg("could not open parquet file")));

    if (state->filters)
    {
        ListCell *lc;
        foreach(lc, state->filters)
        {
            IcebergFilter *f = (IcebergFilter *) lfirst(lc);
            if (f->kind == ICEBERG_FILTER_BETWEEN)
                elog(DEBUG1, "filter: %s BETWEEN %s AND %s", f->column, f->val1, f->val2);
            else
                elog(DEBUG1, "filter: %s %s %s", f->column, f->op, f->val1);
        }
    }
    if (state->columns)
    {
        ListCell *lc;
        foreach(lc, state->columns)
            elog(DEBUG1, "project column: %s", (char *) lfirst(lc));
    }

    node->fdw_state = (void *) state;
}

static TupleTableSlot *
icebergcIterateForeignScan(ForeignScanState *node)
{
    IcebergScanState *state = (IcebergScanState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    int natts = slot->tts_tupleDescriptor->natts;

    ExecClearTuple(slot);

    if (!parquet_reader_next(state->reader, state->values, natts))
        return slot;

    Datum *values = slot->tts_values;
    bool *nulls = slot->tts_isnull;

    for (int i = 0; i < natts; i++)
    {
        if (state->values[i] == NULL)
        {
            nulls[i] = true;
        }
        else
        {
            values[i] = InputFunctionCall(&(state->attinmeta->attinfuncs[i]),
                                          state->values[i],
                                          state->attinmeta->attioparams[i],
                                          state->attinmeta->atttypmods[i]);
            nulls[i] = false;
            pfree(state->values[i]);
            state->values[i] = NULL;
        }
    }

    ExecStoreVirtualTuple(slot);
    return slot;
}

static void
icebergcEndForeignScan(ForeignScanState *node)
{
    IcebergScanState *state = (IcebergScanState *) node->fdw_state;
    if (state)
    {
        if (state->reader)
            parquet_reader_close(state->reader);
        if (state->values)
        {
            TupleDesc desc = RelationGetDescr(node->ss.ss_currentRelation);
            for (int i = 0; i < desc->natts; i++)
                if (state->values[i])
                    pfree(state->values[i]);
            pfree(state->values);
        }
        ListCell *lc;
        foreach(lc, state->filters)
        {
            IcebergFilter *f = (IcebergFilter *) lfirst(lc);
            pfree(f->column);
            if (f->op)
                pfree(f->op);
            if (f->val1)
                pfree(f->val1);
            if (f->val2)
                pfree(f->val2);
            pfree(f);
        }
        list_free(state->filters);
        foreach(lc, state->columns)
            pfree(lfirst(lc));
        list_free(state->columns);
        if (state->opts)
            pfree(state->opts);
        pfree(state);
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

