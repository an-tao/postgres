/*-------------------------------------------------------------------------
 *
 * stat_ext.c
 *	  POSTGRES extended statistics
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/statistics/stat_ext.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic_ext.h"
#include "nodes/relation.h"
#include "statistics/stat_ext_internal.h"
#include "statistics/stats.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"


typedef struct StatExtEntry
{
	Oid			relid;		/* owning relation XXX useless? */
	Oid			statOid;	/* OID of pg_statistic_ext entry */
	int2vector *columns;	/* columns */
	List	   *types;		/* enabled types */
} StatExtEntry;


static List *fetch_statentries_for_relation(Relation pg_statext, Oid relid);
static VacAttrStats **lookup_var_attr_stats(int2vector *attrs,
					  int natts, VacAttrStats **vacattrstats);
static void statext_store(Relation pg_stext, Oid relid,
			  MVNDistinct ndistinct,
			  int2vector *attrs, VacAttrStats **stats);


/*
 * Compute requested extended stats, using the rows sampled for the plain
 * (single-column) stats.
 *
 * This fetches a list of stats from pg_statistic_ext, computes the stats
 * and serializes them back into the catalog (as bytea values).
 */
void
BuildRelationExtStatistics(Relation onerel, double totalrows,
						   int numrows, HeapTuple *rows,
						   int natts, VacAttrStats **vacattrstats)
{
	Relation	pg_stext;
	ListCell   *lc;
	List	   *stats;

	pg_stext = heap_open(StatisticExtRelationId, RowExclusiveLock);
	stats = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

	foreach(lc, stats)
	{
		StatExtEntry   *stat = (StatExtEntry *) lfirst(lc);
		MVNDistinct		ndistinct = NULL;
		VacAttrStats  **stats;
		ListCell	   *lc2;

		/* filter only the interesting vacattrstats records */
		stats = lookup_var_attr_stats(stat->columns, natts, vacattrstats);

		/* check allowed number of dimensions */
		Assert((stat->columns->dim1 >= 2) &&
			   (stat->columns->dim1 <= STATS_MAX_DIMENSIONS));

		/* compute statistic of each type */
		foreach(lc2, stat->types)
		{
			char	t = (char) lfirst_int(lc2);

			if (t == STATS_EXT_NDISTINCT)
				ndistinct = statext_ndistinct_build(totalrows, numrows, rows,
													stat->columns, stats);
		}

		/* store the statistics in the catalog */
		statext_store(pg_stext, stat->statOid, ndistinct,
					  stat->columns, stats);
	}

	heap_close(pg_stext, RowExclusiveLock);
}

/*
 * statext_is_kind_built
 *		Is this stat kind built in the given pg_statistic_ext tuple?
 */
bool
statext_is_kind_built(HeapTuple htup, char type)
{
	AttrNumber  attnum;

	switch (type)
	{
		case STATS_EXT_NDISTINCT:
			attnum = Anum_pg_statistic_ext_standistinct;
			break;

		default:
			elog(ERROR, "unexpected statistics type requested: %d", type);
	}

	return !heap_attisnull(htup, attnum);
}

/*
 * Return a list (of StatExtEntry) of statistics enabled for the given relation.
 */
static List *
fetch_statentries_for_relation(Relation pg_statext, Oid relid)
{
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple   htup;
	List       *result = NIL;

	/*
	 * Prepare to scan pg_statistic_ext for entries having indrelid = this
	 * rel.
	 */
	ScanKeyInit(&skey,
				Anum_pg_statistic_ext_starelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_statext, StatisticExtRelidIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		StatExtEntry *entry;
		Datum		datum;
		bool		isnull;
		int			i;
		ArrayType  *arr;
		char	   *enabled;

		entry = palloc0(sizeof(StatExtEntry));
		entry->relid = relid;
		entry->statOid = HeapTupleGetOid(htup);
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stakeys, &isnull);
		Assert(!isnull);
		arr = DatumGetArrayTypeP(datum);
		entry->columns = buildint2vector((int16 *) ARR_DATA_PTR(arr),
										 ARR_DIMS(arr)[0]);

		/* decode the staenabled char array into a list of chars */
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_staenabled, &isnull);
		Assert(!isnull);
		arr = DatumGetArrayTypeP(datum);
		if (ARR_NDIM(arr) != 1 ||
			ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != CHAROID)
			elog(ERROR, "staenabled is not a 1-D char array");
		enabled = (char *) ARR_DATA_PTR(arr);
		for (i = 0; i < ARR_DIMS(arr)[0]; i++)
		{
			Assert(enabled[i] == STATS_EXT_NDISTINCT);
			entry->types = lappend_int(entry->types, (int) enabled[i]);
		}

		result = lappend(result, entry);
	}

	systable_endscan(scan);

	return result;
}

/*
 * Lookup the VacAttrStats info for the selected columns, with indexes
 * matching the attrs vector (to make it easy to work with when
 * computing extended stats).
 */
static VacAttrStats **
lookup_var_attr_stats(int2vector *attrs, int natts, VacAttrStats **vacattrstats)
{
	int			i;
	int			numattrs;
	VacAttrStats **stats;

	numattrs = attrs->dim1;
	stats = (VacAttrStats **) palloc0(numattrs * sizeof(VacAttrStats *));

	/* lookup VacAttrStats info for the requested columns (same attnum) */
	for (i = 0; i < numattrs; i++)
	{
		int		j;

		stats[i] = NULL;
		for (j = 0; j < natts; j++)
		{
			if (attrs->values[i] == vacattrstats[j]->tupattnum)
			{
				stats[i] = vacattrstats[j];
				break;
			}
		}

		/*
		 * Check that we found a non-dropped column, that the attnum matches
		 * and that there's the requested 'lt' operator and that the type is
		 * 'passed-by-value'.
		 */
		Assert(!stats[i]->attr->attisdropped);
		Assert(stats[i]->tupattnum == attrs->values[i]);

		/*
		 * FIXME This is rather ugly way to check for 'ltopr' (which is
		 * defined for 'scalar' attributes).
		 */
		Assert(((StdAnalyzeData *) stats[i]->extra_data)->ltopr != InvalidOid);
	}

	return stats;
}

/*
 * statext_store
 *	Serializes the statistics and stores them into the pg_statistic_ext tuple.
 */
static void
statext_store(Relation pg_stext, Oid statOid,
			  MVNDistinct ndistinct,
			  int2vector *attrs, VacAttrStats **stats)
{
	HeapTuple	stup,
				oldtup;
	Datum		values[Natts_pg_statistic_ext];
	bool		nulls[Natts_pg_statistic_ext];
	bool		replaces[Natts_pg_statistic_ext];

	memset(nulls, 1, Natts_pg_statistic_ext * sizeof(bool));
	memset(replaces, 0, Natts_pg_statistic_ext * sizeof(bool));
	memset(values, 0, Natts_pg_statistic_ext * sizeof(Datum));

	/*
	 * Construct a new pg_statistic_ext tuple - replace only the histogram and
	 * MCV list, depending whether it actually was computed.
	 */
	if (ndistinct != NULL)
	{
		bytea	   *data = statext_ndistinct_serialize(ndistinct);

		nulls[Anum_pg_statistic_ext_standistinct - 1] = (data == NULL);
		values[Anum_pg_statistic_ext_standistinct - 1] = PointerGetDatum(data);
	}

	/* always replace the value (either by bytea or NULL) */
	replaces[Anum_pg_statistic_ext_standistinct - 1] = true;

	/* always change the availability flags */
	nulls[Anum_pg_statistic_ext_stakeys - 1] = false;

	/* use the new attnums, in case we removed some dropped ones */
	replaces[Anum_pg_statistic_ext_stakeys - 1] = true;

	values[Anum_pg_statistic_ext_stakeys - 1] = PointerGetDatum(attrs);

	/* Is there already a pg_statistic_ext tuple for this attribute? */
	oldtup = SearchSysCache1(STATEXTOID,
							 ObjectIdGetDatum(statOid));

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for extended statistics %u", statOid);

	/* replace it */
	stup = heap_modify_tuple(oldtup,
							 RelationGetDescr(pg_stext),
							 values,
							 nulls,
							 replaces);
	ReleaseSysCache(oldtup);
	CatalogTupleUpdate(pg_stext, &stup->t_self, stup);

	heap_freetuple(stup);
}

/* multi-variate stats comparator */

/*
 * qsort_arg comparator for sorting Datums (MV stats)
 *
 * This does not maintain the tupnoLink array.
 */
int
compare_scalars_simple(const void *a, const void *b, void *arg)
{
	Datum		da = *(Datum *) a;
	Datum		db = *(Datum *) b;
	SortSupport ssup = (SortSupport) arg;

	return ApplySortComparator(da, false, db, false, ssup);
}

/*
 * qsort_arg comparator for sorting data when partitioning a MV bucket
 */
int
compare_scalars_partition(const void *a, const void *b, void *arg)
{
	Datum		da = ((ScalarItem *) a)->value;
	Datum		db = ((ScalarItem *) b)->value;
	SortSupport ssup = (SortSupport) arg;

	return ApplySortComparator(da, false, db, false, ssup);
}

/* initialize multi-dimensional sort */
MultiSortSupport
multi_sort_init(int ndims)
{
	MultiSortSupport mss;

	Assert(ndims >= 2);

	mss = (MultiSortSupport) palloc0(offsetof(MultiSortSupportData, ssup)
									 +sizeof(SortSupportData) * ndims);

	mss->ndims = ndims;

	return mss;
}

/*
 * Prepare sort support info for dimension 'dim' (index into vacattrstats) to
 * 'mss', at the position 'sortdim'
 */
void
multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
						 int dim, VacAttrStats **vacattrstats)
{
	/* first, lookup StdAnalyzeData for the dimension (attribute) */
	SortSupportData ssup;
	StdAnalyzeData *tmp = (StdAnalyzeData *) vacattrstats[dim]->extra_data;

	Assert(mss != NULL);
	Assert(sortdim < mss->ndims);

	/* initialize sort support, etc. */
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;

	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(tmp->ltopr, &ssup);

	mss->ssup[sortdim] = ssup;
}

/* compare all the dimensions in the selected order */
int
multi_sort_compare(const void *a, const void *b, void *arg)
{
	int			i;
	SortItem   *ia = (SortItem *) a;
	SortItem   *ib = (SortItem *) b;

	MultiSortSupport mss = (MultiSortSupport) arg;

	for (i = 0; i < mss->ndims; i++)
	{
		int			compare;

		compare = ApplySortComparator(ia->values[i], ia->isnull[i],
									  ib->values[i], ib->isnull[i],
									  &mss->ssup[i]);

		if (compare != 0)
			return compare;
	}

	/* equal by default */
	return 0;
}

/* compare selected dimension */
int
multi_sort_compare_dim(int dim, const SortItem *a, const SortItem *b,
					   MultiSortSupport mss)
{
	return ApplySortComparator(a->values[dim], a->isnull[dim],
							   b->values[dim], b->isnull[dim],
							   &mss->ssup[dim]);
}

int
multi_sort_compare_dims(int start, int end,
						const SortItem *a, const SortItem *b,
						MultiSortSupport mss)
{
	int			dim;

	for (dim = start; dim <= end; dim++)
	{
		int			r = ApplySortComparator(a->values[dim], a->isnull[dim],
											b->values[dim], b->isnull[dim],
											&mss->ssup[dim]);

		if (r != 0)
			return r;
	}

	return 0;
}
