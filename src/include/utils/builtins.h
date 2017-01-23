/*-------------------------------------------------------------------------
 *
 * builtins.h
 *	  Declarations for operations on built-in types.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/builtins.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUILTINS_H
#define BUILTINS_H

#include "fmgr.h"
#include "nodes/nodes.h"
#include "utils/fmgrprotos.h"


/* bool.c */
extern bool parse_bool(const char *value, bool *result);
extern bool parse_bool_with_len(const char *value, size_t len, bool *result);

/* domains.c */
extern void domain_check(Datum value, bool isnull, Oid domainType,
			 void **extra, MemoryContext mcxt);
extern int	errdatatype(Oid datatypeOid);
extern int	errdomainconstraint(Oid datatypeOid, const char *conname);

/* encode.c */
extern unsigned hex_encode(const char *src, unsigned len, char *dst);
extern unsigned hex_decode(const char *src, unsigned len, char *dst);

/* int.c */
extern int2vector *buildint2vector(const int16 *int2s, int n);

/* name.c */
extern int	namecpy(Name n1, Name n2);
extern int	namestrcpy(Name name, const char *str);
extern int	namestrcmp(Name name, const char *str);

/* numutils.c */
extern int32 pg_atoi(const char *s, int size, int c);
extern void pg_itoa(int16 i, char *a);
extern void pg_ltoa(int32 l, char *a);
extern void pg_lltoa(int64 ll, char *a);
extern char *pg_ltostr_zeropad(char *str, int32 value, int32 minwidth);
extern char *pg_ltostr(char *str, int32 value);
extern uint64 pg_strtouint64(const char *str, char **endptr, int base);

/* float.c */
extern PGDLLIMPORT int extra_float_digits;

extern double get_float8_infinity(void);
extern float get_float4_infinity(void);
extern double get_float8_nan(void);
extern float get_float4_nan(void);
extern int	is_infinite(double val);
extern double float8in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string);
extern char *float8out_internal(double num);
extern int	float4_cmp_internal(float4 a, float4 b);
extern int	float8_cmp_internal(float8 a, float8 b);

/* oid.c */
extern oidvector *buildoidvector(const Oid *oids, int n);
extern Oid	oidparse(Node *node);
extern int	oid_cmp(const void *p1, const void *p2);

/* regexp.c */
extern char *regexp_fixed_prefix(text *text_re, bool case_insensitive,
					Oid collation, bool *exact);

/* ruleutils.c */
extern bool quote_all_identifiers;
extern const char *quote_identifier(const char *ident);
extern char *quote_qualified_identifier(const char *qualifier,
						   const char *ident);

/* varchar.c */
extern int	bpchartruelen(char *s, int len);

/* popular functions from varlena.c */
extern text *cstring_to_text(const char *s);
extern text *cstring_to_text_with_len(const char *s, int len);
extern char *text_to_cstring(const text *t);
extern void text_to_cstring_buffer(const text *src, char *dst, size_t dst_len);

#define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
#define TextDatumGetCString(d) text_to_cstring((text *) DatumGetPointer(d))

/* xid.c */
extern int	xidComparator(const void *arg1, const void *arg2);

/* inet_cidr_ntop.c */
extern char *inet_cidr_ntop(int af, const void *src, int bits,
			   char *dst, size_t size);

/* inet_net_pton.c */
extern int inet_net_pton(int af, const char *src,
			  void *dst, size_t size);

/* network.c */
extern double convert_network_to_scalar(Datum value, Oid typid);
extern Datum network_scan_first(Datum in);
extern Datum network_scan_last(Datum in);
extern void clean_ipv6_addr(int addr_family, char *addr);

/* numeric.c */
extern Datum numeric_float8_no_overflow(PG_FUNCTION_ARGS);

/* format_type.c */
extern char *format_type_be(Oid type_oid);
extern char *format_type_be_qualified(Oid type_oid);
extern char *format_type_with_typemod(Oid type_oid, int32 typemod);
extern char *format_type_with_typemod_qualified(Oid type_oid, int32 typemod);
extern int32 type_maximum_size(Oid type_oid, int32 typemod);

/* quote.c */
extern char *quote_literal_cstr(const char *rawstr);
extern Datum quote_nullable(PG_FUNCTION_ARGS);

/* guc.c */
extern Datum show_config_by_name(PG_FUNCTION_ARGS);
extern Datum show_config_by_name_missing_ok(PG_FUNCTION_ARGS);
extern Datum set_config_by_name(PG_FUNCTION_ARGS);
extern Datum show_all_settings(PG_FUNCTION_ARGS);
extern Datum show_all_file_settings(PG_FUNCTION_ARGS);

/* pg_config.c */
extern Datum pg_config(PG_FUNCTION_ARGS);

/* pg_controldata.c */
extern Datum pg_control_checkpoint(PG_FUNCTION_ARGS);
extern Datum pg_control_system(PG_FUNCTION_ARGS);
extern Datum pg_control_init(PG_FUNCTION_ARGS);
extern Datum pg_control_recovery(PG_FUNCTION_ARGS);

/* rls.c */
extern Datum row_security_active(PG_FUNCTION_ARGS);
extern Datum row_security_active_name(PG_FUNCTION_ARGS);

/* lockfuncs.c */
extern Datum pg_lock_status(PG_FUNCTION_ARGS);
extern Datum pg_blocking_pids(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_all(PG_FUNCTION_ARGS);

/* txid.c */
extern Datum txid_snapshot_in(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_out(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_recv(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_send(PG_FUNCTION_ARGS);
extern Datum txid_current(PG_FUNCTION_ARGS);
extern Datum txid_current_if_assigned(PG_FUNCTION_ARGS);
extern Datum txid_current_snapshot(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_xmin(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_xmax(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_xip(PG_FUNCTION_ARGS);
extern Datum txid_visible_in_snapshot(PG_FUNCTION_ARGS);
extern Datum txid_status(PG_FUNCTION_ARGS);

/* uuid.c */
extern Datum uuid_in(PG_FUNCTION_ARGS);
extern Datum uuid_out(PG_FUNCTION_ARGS);
extern Datum uuid_send(PG_FUNCTION_ARGS);
extern Datum uuid_recv(PG_FUNCTION_ARGS);
extern Datum uuid_lt(PG_FUNCTION_ARGS);
extern Datum uuid_le(PG_FUNCTION_ARGS);
extern Datum uuid_eq(PG_FUNCTION_ARGS);
extern Datum uuid_ge(PG_FUNCTION_ARGS);
extern Datum uuid_gt(PG_FUNCTION_ARGS);
extern Datum uuid_ne(PG_FUNCTION_ARGS);
extern Datum uuid_cmp(PG_FUNCTION_ARGS);
extern Datum uuid_sortsupport(PG_FUNCTION_ARGS);
extern Datum uuid_hash(PG_FUNCTION_ARGS);

/* windowfuncs.c */
extern Datum window_row_number(PG_FUNCTION_ARGS);
extern Datum window_rank(PG_FUNCTION_ARGS);
extern Datum window_dense_rank(PG_FUNCTION_ARGS);
extern Datum window_percent_rank(PG_FUNCTION_ARGS);
extern Datum window_cume_dist(PG_FUNCTION_ARGS);
extern Datum window_ntile(PG_FUNCTION_ARGS);
extern Datum window_lag(PG_FUNCTION_ARGS);
extern Datum window_lag_with_offset(PG_FUNCTION_ARGS);
extern Datum window_lag_with_offset_and_default(PG_FUNCTION_ARGS);
extern Datum window_lead(PG_FUNCTION_ARGS);
extern Datum window_lead_with_offset(PG_FUNCTION_ARGS);
extern Datum window_lead_with_offset_and_default(PG_FUNCTION_ARGS);
extern Datum window_first_value(PG_FUNCTION_ARGS);
extern Datum window_last_value(PG_FUNCTION_ARGS);
extern Datum window_nth_value(PG_FUNCTION_ARGS);

/* access/spgist/spgquadtreeproc.c */
extern Datum spg_quad_config(PG_FUNCTION_ARGS);
extern Datum spg_quad_choose(PG_FUNCTION_ARGS);
extern Datum spg_quad_picksplit(PG_FUNCTION_ARGS);
extern Datum spg_quad_inner_consistent(PG_FUNCTION_ARGS);
extern Datum spg_quad_leaf_consistent(PG_FUNCTION_ARGS);

/* access/spgist/spgkdtreeproc.c */
extern Datum spg_kd_config(PG_FUNCTION_ARGS);
extern Datum spg_kd_choose(PG_FUNCTION_ARGS);
extern Datum spg_kd_picksplit(PG_FUNCTION_ARGS);
extern Datum spg_kd_inner_consistent(PG_FUNCTION_ARGS);

/* access/spgist/spgtextproc.c */
extern Datum spg_text_config(PG_FUNCTION_ARGS);
extern Datum spg_text_choose(PG_FUNCTION_ARGS);
extern Datum spg_text_picksplit(PG_FUNCTION_ARGS);
extern Datum spg_text_inner_consistent(PG_FUNCTION_ARGS);
extern Datum spg_text_leaf_consistent(PG_FUNCTION_ARGS);

/* access/gin/ginarrayproc.c */
extern Datum ginarrayextract(PG_FUNCTION_ARGS);
extern Datum ginarrayextract_2args(PG_FUNCTION_ARGS);
extern Datum ginqueryarrayextract(PG_FUNCTION_ARGS);
extern Datum ginarrayconsistent(PG_FUNCTION_ARGS);
extern Datum ginarraytriconsistent(PG_FUNCTION_ARGS);

/* access/tablesample/bernoulli.c */
extern Datum tsm_bernoulli_handler(PG_FUNCTION_ARGS);

/* access/tablesample/system.c */
extern Datum tsm_system_handler(PG_FUNCTION_ARGS);

/* access/transam/twophase.c */
extern Datum pg_prepared_xact(PG_FUNCTION_ARGS);

/* access/transam/multixact.c */
extern Datum pg_get_multixact_members(PG_FUNCTION_ARGS);

/* access/transam/committs.c */
extern Datum pg_xact_commit_timestamp(PG_FUNCTION_ARGS);
extern Datum pg_last_committed_xact(PG_FUNCTION_ARGS);

/* catalogs/dependency.c */
extern Datum pg_describe_object(PG_FUNCTION_ARGS);
extern Datum pg_identify_object(PG_FUNCTION_ARGS);
extern Datum pg_identify_object_as_address(PG_FUNCTION_ARGS);

/* catalog/objectaddress.c */
extern Datum pg_get_object_address(PG_FUNCTION_ARGS);

/* commands/constraint.c */
extern Datum unique_key_recheck(PG_FUNCTION_ARGS);

/* commands/event_trigger.c */
extern Datum pg_event_trigger_dropped_objects(PG_FUNCTION_ARGS);
extern Datum pg_event_trigger_table_rewrite_oid(PG_FUNCTION_ARGS);
extern Datum pg_event_trigger_table_rewrite_reason(PG_FUNCTION_ARGS);
extern Datum pg_event_trigger_ddl_commands(PG_FUNCTION_ARGS);

/* commands/extension.c */
extern Datum pg_available_extensions(PG_FUNCTION_ARGS);
extern Datum pg_available_extension_versions(PG_FUNCTION_ARGS);
extern Datum pg_extension_update_paths(PG_FUNCTION_ARGS);
extern Datum pg_extension_config_dump(PG_FUNCTION_ARGS);

/* commands/prepare.c */
extern Datum pg_prepared_statement(PG_FUNCTION_ARGS);

/* utils/mmgr/portalmem.c */
extern Datum pg_cursor(PG_FUNCTION_ARGS);

#endif   /* BUILTINS_H */
