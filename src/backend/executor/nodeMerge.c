/*-------------------------------------------------------------------------
 *
 * nodeMerge.c
 *	  routines to handle Merge nodes.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMerge.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeMerge.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tqual.h"


/*
 * Check and execute the first qualifying MATCHED action. The current target
 * tuple is identified by tupleid.
 *
 * We start from the first WHEN MATCHED action and check if the additional WHEN
 * quals pass. If  the additional quals for the first action do not pass, we
 * check the second, then the third and so on. If we reach to the end, no
 * action is taken and we return true, indicating that no further action is
 * required for this tuple.
 *
 * If we do find a qualifying action, then we attempt the given action. In case
 * the tuple is concurrently updated, EvalPlanQual is run with the updated
 * tuple to recheck the join quals. Note that the additional quals associated
 * with individual actions are evaluated separately by the MERGE code, while
 * EvalPlanQual checks for the join quals. If EvalPlanQual tells us that the
 * updated tuple still passes the join quals, then we restart from the top and
 * again look for a qualifying action. Otherwise, we return false indicating
 * that a NOT MATCHED action must now be executed for the current source tuple.
 */
static bool
ExecMergeMatched(ModifyTableState *mtstate, EState *estate,
				 TupleTableSlot *slot, JunkFilter *junkfilter,
				 ItemPointer tupleid)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	int			ud_target;
	bool		isNull;
	Datum		datum;
	Oid			tableoid = InvalidOid;
	List	   *mergeMatchedActionStateList = NIL;
	HeapUpdateFailureData hufd;
	bool		tuple_updated,
				tuple_deleted;
	Buffer		buffer;
	HeapTupleData tuple;
	EPQState   *epqstate = &mtstate->mt_epqstate;
	ResultRelInfo *saved_resultRelInfo;
	ResultRelInfo *resultRelInfo;
	ListCell   *l;
	TupleTableSlot *saved_slot = slot;

	/*
	 * We always fetch the tableoid while performing MATCHED MERGE action.
	 * This is strictly not required if the target table is not a partitioned
	 * table. But we are not yet optimising for that case.
	 */
	datum = ExecGetJunkAttribute(slot, junkfilter->jf_otherJunkAttNo,
								 &isNull);
	Assert(!isNull);
	tableoid = DatumGetObjectId(datum);

	/*
	 * If we're dealing with a MATCHED tuple, then tableoid must have been set
	 * correctly. In case of partitioned table, we must now fetch the correct
	 * result relation corresponding to the child table emitting the matching
	 * target row. For normal table, there is just one result relation and it
	 * must be the one emitting the matching row.
	 */
	for (ud_target = 0; ud_target < mtstate->mt_nplans; ud_target++)
	{
		ResultRelInfo *currRelInfo = mtstate->resultRelInfo + ud_target;
		Oid			relid = RelationGetRelid(currRelInfo->ri_RelationDesc);

		if (tableoid == relid)
			break;
	}

	/* We must have found the child result relation. */
	Assert(ud_target < mtstate->mt_nplans);
	resultRelInfo = mtstate->resultRelInfo + ud_target;

	/*
	 * Save the current information and work with the correct result relation.
	 */
	saved_resultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	/*
	 * And get the correct action lists.
	 */
	mergeMatchedActionStateList = (List *)
		list_nth(mtstate->mt_mergeMatchedActionStateLists, ud_target);

	/*
	 * If there are not WHEN MATCHED actions, we are done.
	 */
	if (mergeMatchedActionStateList == NIL)
		return true;

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject. The target's existing tuple is installed in the scantuple.
	 * Again, this target relation's slot is required only in the case of a
	 * MATCHED tuple and UPDATE/DELETE actions.
	 */
	econtext->ecxt_scantuple = mtstate->mt_merge_existing[ud_target];
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

lmerge_matched:;
	slot = saved_slot;
	buffer = InvalidBuffer;

	/*
	 * UPDATE/DELETE is only invoked for matched rows. And we must have found
	 * the tupleid of the target row in that case. We fetch using SnapshotAny
	 * because we might get called again after EvalPlanQual returns us a new
	 * tuple. This tuple may not be visible to our MVCC snapshot.
	 */
	Assert(tupleid != NULL);

	tuple.t_self = *tupleid;
	if (!heap_fetch(resultRelInfo->ri_RelationDesc, SnapshotAny, &tuple,
					&buffer, true, NULL))
		elog(ERROR, "Failed to fetch the target tuple");

	/* Store target's existing tuple in the state's dedicated slot */
	ExecStoreTuple(&tuple, mtstate->mt_merge_existing[ud_target], buffer,
				   false);

	foreach(l, mergeMatchedActionStateList)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);

		/*
		 * Test condition, if any
		 *
		 * In the absence of a condition we perform the action unconditionally
		 * (no need to check separately since ExecQual() will return true if
		 * there are no conditions to evaluate).
		 */
		if (!ExecQual(action->whenqual, econtext))
			continue;

		/*
		 * If we found a DO NOTHING action, we're done.
		 */
		if (action->commandType == CMD_NOTHING)
			break;

		/*
		 * Check if the existing target tuple meet the USING checks of
		 * UPDATE/DELETE RLS policies. If those checks fail, we throw an
		 * error.
		 *
		 * The WITH CHECK quals are applied in ExecUpdate() and hence we need
		 * not do anything special to handle them.
		 *
		 * NOTE: We must do this after WHEN quals are evaluated so that we
		 * check policies only when they matter.
		 */
		if (resultRelInfo->ri_WithCheckOptions)
		{
			ExecWithCheckOptions(action->commandType == CMD_UPDATE ?
								 WCO_RLS_MERGE_UPDATE_CHECK : WCO_RLS_MERGE_DELETE_CHECK,
								 resultRelInfo,
								 mtstate->mt_merge_existing[ud_target],
								 mtstate->ps.state);
		}

		/* Perform stated action */
		switch (action->commandType)
		{
			case CMD_UPDATE:

				/*
				 * We set up the projection earlier, so all we do here is
				 * Project, no need for any other tasks prior to the
				 * ExecUpdate.
				 */
				ExecProject(action->proj);

				/*
				 * We don't call ExecFilterJunk() because the projected tuple
				 * using the UPDATE action's targetlist doesn't have a junk
				 * attribute.
				 */

				slot = ExecUpdate(mtstate, tupleid, NULL,
								  action->slot, slot, epqstate, estate,
								  &tuple_updated, &hufd,
								  action, mtstate->canSetTag);
				break;

			case CMD_DELETE:
				/* Nothing to Project for a DELETE action */
				slot = ExecDelete(mtstate, tupleid, NULL,
								  slot, epqstate, estate,
								  &tuple_deleted, false, &hufd, action,
								  mtstate->canSetTag);

				break;

			case CMD_NOTHING:
				/* Must have been handled already */
				break;
			default:
				elog(ERROR, "unknown action in MERGE WHEN MATCHED clause");

		}

		/*
		 * Check for any concurrent update/delete operation which may have
		 * prevented our update/delete. We also check for situations where we
		 * might be trying to update/delete the same tuple twice.
		 */
		if ((action->commandType == CMD_UPDATE && !tuple_updated) ||
			(action->commandType == CMD_DELETE && !tuple_deleted))

		{
			switch (hufd.result)
			{
				case HeapTupleMayBeUpdated:
					break;
				case HeapTupleInvisible:

					/*
					 * This state should never be reached since the underlying
					 * JOIN runs with a MVCC snapshot and should only return
					 * rows visible to us.
					 */
					elog(ERROR, "unexpected invisible tuple");
					break;

				case HeapTupleSelfUpdated:

					/*
					 * SQLStandard disallows this for MERGE.
					 */
					if (TransactionIdIsCurrentTransactionId(hufd.xmax))
						ereport(ERROR,
								(errcode(ERRCODE_CARDINALITY_VIOLATION),
								 errmsg("MERGE command cannot affect row a second time"),
								 errhint("Ensure that not more than one source rows match any one target row")));
					/* This shouldn't happen */
					elog(ERROR, "attempted to update or delete invisible tuple");
					break;

				case HeapTupleUpdated:

					/*
					 * The target tuple was concurrently updated/deleted by
					 * some other transaction.
					 *
					 * If the current tuple is that last tuple in the update
					 * chain, then we know that the tuple was concurrently
					 * deleted. Just return and let the caller try NOT MATCHED
					 * actions.
					 *
					 * If the current tuple was concurrently updated, then we
					 * must run the EvalPlanQual() with the new version of the
					 * tuple. If EvalPlanQual() does not return a tuple (can
					 * that happen?), then again we switch to NOT MATCHED
					 * action. If it does return a tuple and the join qual is
					 * still satified, then we just need to recheck the
					 * MATCHED actions, starting from the top, and execute the
					 * first qualifying action.
					 */
					if (!ItemPointerEquals(tupleid, &hufd.ctid))
					{
						TupleTableSlot *epqslot;

						/*
						 * Since we generate a JOIN query with a target table
						 * RTE different than the result relation RTE, we must
						 * pass in the RTI of the relation used in the join
						 * query and not the one from result relation.
						 */
						epqslot = EvalPlanQual(estate,
											   epqstate,
											   resultRelInfo->ri_RelationDesc,
											   resultRelInfo->ri_mergeTargetRTI,
											   LockTupleExclusive,
											   &hufd.ctid,
											   hufd.xmax);

						if (!TupIsNull(epqslot))
						{
							(void) ExecGetJunkAttribute(epqslot,
														resultRelInfo->ri_junkFilter->jf_junkAttNo,
														&isNull);

							/*
							 * A valid ctid means that we are still dealing
							 * with MATCHED case. But we must retry from the
							 * start with the updated tuple to ensure that the
							 * first qualifying WHEN MATCHED action is
							 * executed.
							 *
							 * We don't use the new slot returned by
							 * EvalPlanQual because we anyways re-install the
							 * new target tuple in econtext->ecxt_scantuple
							 * before re-evaluating WHEN AND conditions and
							 * re-projecting the update targetlists. The
							 * source side tuple does not change and hence we
							 * can safely continue to use the old slot.
							 */
							if (!isNull)
							{
								/*
								 * Must update *tupleid to the TID of the
								 * newer tuple found in the update chain.
								 */
								*tupleid = hufd.ctid;
								if (BufferIsValid(buffer))
									ReleaseBuffer(buffer);
								goto lmerge_matched;
							}
						}
					}

					/*
					 * Tell the caller about the updated TID, restore the
					 * state back and return.
					 */
					*tupleid = hufd.ctid;
					estate->es_result_relation_info = saved_resultRelInfo;
					if (BufferIsValid(buffer))
						ReleaseBuffer(buffer);
					return false;

				default:
					break;

			}
		}

		if (action->commandType == CMD_UPDATE && tuple_updated)
			InstrCountFiltered1(&mtstate->ps, 1);
		if (action->commandType == CMD_DELETE && tuple_deleted)
			InstrCountFiltered2(&mtstate->ps, 1);

		/*
		 * We've activated one of the WHEN clauses, so we don't search
		 * further. This is required behaviour, not an optimisation.
		 */
		estate->es_result_relation_info = saved_resultRelInfo;
		break;
	}

	if (BufferIsValid(buffer))
		ReleaseBuffer(buffer);

	/*
	 * Successfully executed an action or no qualifying action was found.
	 */
	return true;
}

/*
 * Execute the first qualifying NOT MATCHED action.
 */
static void
ExecMergeNotMatched(ModifyTableState *mtstate, EState *estate,
					TupleTableSlot *slot)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	List	   *mergeNotMatchedActionStateList = NIL;
	ResultRelInfo *saved_resultRelInfo;
	ResultRelInfo *resultRelInfo;
	ListCell   *l;

	/*
	 * We are dealing with NOT MATCHED tuple. For partitioned table, work with
	 * the root partition. For regular tables, just use the currently active
	 * result relation.
	 */
	resultRelInfo = getTargetResultRelInfo(mtstate);
	saved_resultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	/*
	 * For INSERT actions, any subplan's merge action is OK since the the
	 * INSERT's targetlist and the WHEN conditions can only refer to the
	 * source relation and hence it does not matter which result relation we
	 * work with. So we just choose the first one.
	 */
	mergeNotMatchedActionStateList = (List *)
		list_nth(mtstate->mt_mergeNotMatchedActionStateLists, 0);

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject. The target's existing tuple is installed in the scantuple.
	 * Again, this target relation's slot is required only in the case of a
	 * MATCHED tuple and UPDATE/DELETE actions.
	 */
	econtext->ecxt_scantuple = mtstate->mt_merge_existing[0];
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

	foreach(l, mergeNotMatchedActionStateList)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);

		/*
		 * Test condition, if any
		 *
		 * In the absence of a condition we perform the action unconditionally
		 * (no need to check separately since ExecQual() will return true if
		 * there are no conditions to evaluate).
		 */
		if (!ExecQual(action->whenqual, econtext))
			continue;

		/* Perform stated action */
		switch (action->commandType)
		{
			case CMD_INSERT:

				/*
				 * We set up the projection earlier, so all we do here is
				 * Project, no need for any other tasks prior to the
				 * ExecInsert.
				 */
				ExecProject(action->proj);

				slot = ExecInsert(mtstate, action->slot, slot,
								  NULL, ONCONFLICT_NONE, estate, action,
								  mtstate->canSetTag);
				break;
			case CMD_NOTHING:
				/* Do Nothing */
				break;
			default:
				elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
		}

		/*
		 * We've activated one of the WHEN clauses, so we don't search
		 * further. This is required behaviour, not an optimisation.
		 */
		estate->es_result_relation_info = saved_resultRelInfo;
		break;
	}
}

/*
 * Perform MERGE.
 */
void
ExecMerge(ModifyTableState *mtstate, EState *estate, TupleTableSlot *slot,
		  JunkFilter *junkfilter, ResultRelInfo *resultRelInfo)
{
	ItemPointer tupleid;
	ItemPointerData tuple_ctid;
	bool		matched = false;
	char		relkind;
	Datum		datum;
	bool		isNull;

	relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
	Assert(relkind == RELKIND_RELATION ||
		   relkind == RELKIND_MATVIEW ||
		   relkind == RELKIND_PARTITIONED_TABLE);

	/*
	 * We run a JOIN between the target relation and the source relation to
	 * find a set of candidate source rows that has matching row in the target
	 * table and a set of candidate source rows that does not have matching
	 * row in the target table. If the join returns us a tuple with target
	 * relation's tid set, that implies that the join found a matching row for
	 * the given source tuple. This case triggers the WHEN MATCHED clause of
	 * the MERGE. Whereas a NULL in the target relation's ctid column
	 * indicates a NOT MATCHED case.
	 */
	datum = ExecGetJunkAttribute(slot, junkfilter->jf_junkAttNo, &isNull);

	if (!isNull)
	{
		matched = true;
		tupleid = (ItemPointer) DatumGetPointer(datum);
		tuple_ctid = *tupleid;	/* be sure we don't free ctid!! */
		tupleid = &tuple_ctid;
	}
	else
	{
		matched = false;
		tupleid = NULL;			/* we don't need it for INSERT actions */
	}

	/*
	 * If we are dealing with a WHEN MATCHED case, we look at the given WHEN
	 * MATCHED actions in an order and execute the first action which also
	 * satisfies the additional WHEN MATCHED AND quals. If an action without
	 * any additional quals is found, that action is executed.
	 *
	 * Similarly, if we are dealing with WHEN NOT MATCHED case, we look at the
	 * given WHEN NOT MATCHED actions in an ordr and execute the first
	 * qualifying action.
	 *
	 * Things get interesting in case of concurrent update/delete of the
	 * target tuple. Such concurrent update/delete is detected while we are
	 * executing a WHEN MATCHED action.
	 *
	 * A concurrent update for example can:
	 *
	 * 1. modify the target tuple so that it no longer satisfies the
	 * additional quals attached to the current WHEN MATCHED action OR 2.
	 * modify the target tuple so that the join quals no longer pass and hence
	 * the source tuple no longer has a match.
	 *
	 * In the first case, we are still dealing with a WHEN MATCHED case, but
	 * we should recheck the list of WHEN MATCHED actions and choose the first
	 * one that satisfies the new target tuple. In the second case, since the
	 * source tuple no longer matches the target tuple, we now instead find a
	 * qualifying WHEN NOT MATCHED action and execute that.
	 *
	 * A concurrent delete, changes a WHEN MATCHED case to WHEN NOT MATCHED.
	 *
	 * ExecMergeMatched takes care of following the update chain and
	 * re-finding the qualifying WHEN MATCHED action, as long as the updated
	 * target tuple still satisfies the join quals i.e. it still remains a
	 * WHEN MATCHED case. If the tuple gets deleted or the join quals fail, it
	 * returns and we try ExecMergeNotMatched. Given that ExecMergeMatched
	 * always make progress by following the update chain and we never switch
	 * from ExecMergeNotMatched to ExecMergeMatched, there is no risk of a
	 * livelock.
	 */
	if (!matched ||
		!ExecMergeMatched(mtstate, estate, slot, junkfilter, tupleid))
		ExecMergeNotMatched(mtstate, estate, slot);
}

