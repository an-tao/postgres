/*-------------------------------------------------------------------------
 *
 * statistics.h
 *	  Extended statistics and selectivity estimation functions.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/statistics/statistics.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STATISTICS_H
#define STATISTICS_H

#include "commands/vacuum.h"

#define STATS_MAX_DIMENSIONS	8		/* max number of attributes */

/* Multivariate distinct coefficients */
#define STATS_NDISTINCT_MAGIC		0xA352BFA4	/* struct identifier */
#define STATS_NDISTINCT_TYPE_BASIC	1	/* struct version */

/* MVDistinctItem represents a single combination of columns */
typedef struct MVNDistinctItem
{
	double		ndistinct;		/* ndistinct value for this combination */
	Bitmapset  *attrs;			/* attr numbers of items */
} MVNDistinctItem;

/* A MVNDistinct object, comprising all possible combinations of columns */
typedef struct MVNDistinct
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of ndistinct (BASIC) */
	uint32		nitems;			/* number of items in the statistic */
	MVNDistinctItem items[FLEXIBLE_ARRAY_MEMBER];
} MVNDistinct;

#define STATS_DEPS_MAGIC		0xB4549A2C		/* marks serialized bytea */
#define STATS_DEPS_TYPE_BASIC	1		/* basic dependencies type */

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependencyData
{
	double		degree;			/* degree of validity (0-1) */
	AttrNumber	nattributes;	/* number of attributes */
	AttrNumber	attributes[FLEXIBLE_ARRAY_MEMBER];	/* attribute numbers */
} MVDependencyData;

typedef MVDependencyData *MVDependency;

typedef struct MVDependenciesData
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MV Dependencies (BASIC) */
	uint32		ndeps;			/* number of dependencies */
	MVDependency deps[FLEXIBLE_ARRAY_MEMBER];	/* dependencies */
} MVDependenciesData;

typedef MVDependenciesData *MVDependencies;

extern bool dependency_implies_attribute(MVDependency dependency,
						AttrNumber attnum, int16 *attmap);
extern bool dependency_is_fully_matched(MVDependency dependency,
						Bitmapset *attnums, int16 *attmap);

extern MVNDistinct *statext_ndistinct_load(Oid mvoid);
extern MVDependencies staext_dependencies_load(Oid mvoid);

extern void BuildRelationExtStatistics(Relation onerel, double totalrows,
						   int numrows, HeapTuple *rows,
						   int natts, VacAttrStats **vacattrstats);
extern bool statext_is_kind_built(HeapTuple htup, char kind);

#endif   /* STATISTICS_H */
