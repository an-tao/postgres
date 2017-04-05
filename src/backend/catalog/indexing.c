/*-------------------------------------------------------------------------
 *
 * indexing.c
 *	  This file contains routines to support indexes defined on system
 *	  catalogs.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/indexing.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "executor/executor.h"
#include "utils/rel.h"


/*
 * CatalogOpenIndexes - open the indexes on a system catalog.
 *
 * When inserting or updating tuples in a system catalog, call this
 * to prepare to update the indexes for the catalog.
 *
 * In the current implementation, we share code for opening/closing the
 * indexes with execUtils.c.  But we do not use ExecInsertIndexTuples,
 * because we don't want to create an EState.  This implies that we
 * do not support partial or expressional indexes on system catalogs,
 * nor can we support generalized exclusion constraints.
 * This could be fixed with localized changes here if we wanted to pay
 * the extra overhead of building an EState.
 */
CatalogIndexState
CatalogOpenIndexes(Relation heapRel)
{
	ResultRelInfo *resultRelInfo;

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = heapRel;
	resultRelInfo->ri_TrigDesc = NULL;	/* we don't fire triggers */

	ExecOpenIndices(resultRelInfo, false);

	return resultRelInfo;
}

/*
 * CatalogCloseIndexes - clean up resources allocated by CatalogOpenIndexes
 */
void
CatalogCloseIndexes(CatalogIndexState indstate)
{
	ExecCloseIndices(indstate);
	pfree(indstate);
}

/*
 * CatalogIndexInsert - insert index entries for one catalog tuple
 *
 * This should be called for each inserted or updated catalog tuple.
 *
 * If the tuple was WARM updated, the modified_attrs contains the list of
 * columns updated by the update. We must not insert new index entries for
 * indexes which do not refer to any of the modified columns.
 *
 * This is effectively a cut-down version of ExecInsertIndexTuples.
 */
static void
CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple,
		Bitmapset *modified_attrs, bool warm_update)
{
	int			i;
	int			numIndexes;
	RelationPtr relationDescs;
	Relation	heapRelation;
	TupleTableSlot *slot;
	IndexInfo **indexInfoArray;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ItemPointerData root_tid;

	/*
	 * HOT update does not require index inserts, but WARM may need for some
	 * indexes.
	 */
	if (HeapTupleIsHeapOnly(heapTuple) && !warm_update)
		return;

	/*
	 * If we've done a WARM update, then we must index the TID of the root line
	 * pointer and not the actual TID of the new tuple.
	 */
	if (warm_update)
		ItemPointerSet(&root_tid,
				ItemPointerGetBlockNumber(&(heapTuple->t_self)),
				HeapTupleHeaderGetRootOffset(heapTuple->t_data));
	else
		ItemPointerCopy(&heapTuple->t_self, &root_tid);


	/*
	 * Get information from the state structure.  Fall out if nothing to do.
	 */
	numIndexes = indstate->ri_NumIndices;
	if (numIndexes == 0)
		return;
	relationDescs = indstate->ri_IndexRelationDescs;
	indexInfoArray = indstate->ri_IndexRelationInfo;
	heapRelation = indstate->ri_RelationDesc;

	/* Need a slot to hold the tuple being examined */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));
	ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndexes; i++)
	{
		IndexInfo  *indexInfo;

		indexInfo = indexInfoArray[i];

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/*
		 * If we've done WARM update, then we must not insert a new index tuple
		 * if none of the index keys have changed. This is not just an
		 * optimization, but a requirement for WARM to work correctly.
		 */
		if (warm_update)
		{
			if (!bms_overlap(modified_attrs, indexInfo->ii_indxattrs))
				continue;
		}

		/*
		 * Expressional and partial indexes on system catalogs are not
		 * supported, nor exclusion constraints, nor deferred uniqueness
		 */
		Assert(indexInfo->ii_Expressions == NIL);
		Assert(indexInfo->ii_Predicate == NIL);
		Assert(indexInfo->ii_ExclusionOps == NULL);
		Assert(relationDescs[i]->rd_index->indimmediate);

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   NULL,	/* no expression eval to do */
					   values,
					   isnull);

		/*
		 * The index AM does the rest.
		 */
		index_insert(relationDescs[i],	/* index relation */
					 values,	/* array of index Datums */
					 isnull,	/* is-null flags */
					 &root_tid,
					 heapRelation,
					 relationDescs[i]->rd_index->indisunique ?
					 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
					 indexInfo,
					 warm_update);
	}

	ExecDropSingleTupleTableSlot(slot);
}

/*
 * CatalogTupleInsert - do heap and indexing work for a new catalog tuple
 *
 * Insert the tuple data in "tup" into the specified catalog relation.
 * The Oid of the inserted tuple is returned.
 *
 * This is a convenience routine for the common case of inserting a single
 * tuple in a system catalog; it inserts a new heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleInsertWithInfo in such cases.)
 */
Oid
CatalogTupleInsert(Relation heapRel, HeapTuple tup)
{
	CatalogIndexState indstate;
	Oid			oid;

	indstate = CatalogOpenIndexes(heapRel);

	oid = simple_heap_insert(heapRel, tup);

	CatalogIndexInsert(indstate, tup, NULL, false);
	CatalogCloseIndexes(indstate);

	return oid;
}

/*
 * CatalogTupleInsertWithInfo - as above, but with caller-supplied index info
 *
 * This should be used when it's important to amortize CatalogOpenIndexes/
 * CatalogCloseIndexes work across multiple insertions.  At some point we
 * might cache the CatalogIndexState data somewhere (perhaps in the relcache)
 * so that callers needn't trouble over this ... but we don't do so today.
 */
Oid
CatalogTupleInsertWithInfo(Relation heapRel, HeapTuple tup,
						   CatalogIndexState indstate)
{
	Oid			oid;

	oid = simple_heap_insert(heapRel, tup);

	CatalogIndexInsert(indstate, tup, NULL, false);

	return oid;
}

/*
 * CatalogTupleUpdate - do heap and indexing work for updating a catalog tuple
 *
 * Update the tuple identified by "otid", replacing it with the data in "tup".
 *
 * This is a convenience routine for the common case of updating a single
 * tuple in a system catalog; it updates one heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleUpdateWithInfo in such cases.)
 */
void
CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup)
{
	CatalogIndexState indstate;
	bool	warm_update;
	Bitmapset	*modified_attrs;

	indstate = CatalogOpenIndexes(heapRel);

	simple_heap_update(heapRel, otid, tup, &modified_attrs, &warm_update);

	CatalogIndexInsert(indstate, tup, modified_attrs, warm_update);
	CatalogCloseIndexes(indstate);
}

/*
 * CatalogTupleUpdateWithInfo - as above, but with caller-supplied index info
 *
 * This should be used when it's important to amortize CatalogOpenIndexes/
 * CatalogCloseIndexes work across multiple updates.  At some point we
 * might cache the CatalogIndexState data somewhere (perhaps in the relcache)
 * so that callers needn't trouble over this ... but we don't do so today.
 */
void
CatalogTupleUpdateWithInfo(Relation heapRel, ItemPointer otid, HeapTuple tup,
						   CatalogIndexState indstate)
{
	Bitmapset  *modified_attrs;
	bool		warm_update;

	simple_heap_update(heapRel, otid, tup, &modified_attrs, &warm_update);

	CatalogIndexInsert(indstate, tup, modified_attrs, warm_update);
}

/*
 * CatalogTupleDelete - do heap and indexing work for deleting a catalog tuple
 *
 * Delete the tuple identified by "tid" in the specified catalog.
 *
 * With Postgres heaps, there is no index work to do at deletion time;
 * cleanup will be done later by VACUUM.  However, callers of this function
 * shouldn't have to know that; we'd like a uniform abstraction for all
 * catalog tuple changes.  Hence, provide this currently-trivial wrapper.
 *
 * The abstraction is a bit leaky in that we don't provide an optimized
 * CatalogTupleDeleteWithInfo version, because there is currently nothing to
 * optimize.  If we ever need that, rather than touching a lot of call sites,
 * it might be better to do something about caching CatalogIndexState.
 */
void
CatalogTupleDelete(Relation heapRel, ItemPointer tid)
{
	simple_heap_delete(heapRel, tid);
}
