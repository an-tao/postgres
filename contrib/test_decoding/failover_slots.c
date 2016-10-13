#include "postgres.h"

#include "fmgr.h"

#include "replication/slot.h"

PG_MODULE_MAGIC;

/*
 * Set whether the current slot is a failover slot.
 *
 * If setting failover, force a flush so it is created
 * promptly on the replica.
 *
 * If clearing failover, the slot copy on the replica
 * is left in place. We don't send a delete WAL message.
 * The user might actually have intended to keep it and
 * can drop it on the replica if they didn't.
 *
 * Because this function isn't created by default it
 * doesn't appear in pg_proc, so it doesn't get the usual
 * metadata generated and we have to do it explicitly.
 */
PG_FUNCTION_INFO_V1(slot_set_failover);

Datum
slot_set_failover(PG_FUNCTION_ARGS)
{	
	char *slot_name = NameStr(*PG_GETARG_NAME(0));
	bool failover = PG_GETARG_BOOL(1);

	ReplicationSlotAcquire(slot_name);
	ReplicationSlotAcquiredSetFailover(failover);
	ReplicationSlotRelease();

	PG_RETURN_VOID();
}
