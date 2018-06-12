/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _LOGGER_H_
#define _LOGGER_H_

#define LOG_OK 0
#define LOG_ERR (-1)

#define LOGFILE "log"
#define LOGFILE_SHARED "log_shared"
#define MONETDB_DTX 1
char dbfarmPath[1024];

#define LOG_START	1
#define LOG_END		2
#define LOG_INSERT	3
#define LOG_UPDATE	5
#define LOG_CREATE	6
#define LOG_DESTROY	7
#define LOG_USE		8
#define LOG_CLEAR	9
#define LOG_SEQ		10
/* add log state ; add for distributed transation*/
#define LOG_PREPARE     11
#define LOG_PRE_ABORT   12
#define LOG_PRE_COMMIT  13

#define TMGIDSIZE	22
//char GlobalLoggerGid[TMGIDSIZE];

typedef struct create_clm
{
	int coid;      /* column oid, add for retry create table */
	char cname[1024];
	int toid;
	char type[32];
	int type_digits;
	int type_scale;
	struct create_clm* next;  
}create_clm;

typedef struct create_tab
{
	int toid;     /* table oid, add for retry create table */
	int soid;
	char tname[1024];
	sht type;		/* table, view, etc */
	sht access;		/* writable, readonly, appendonly */
	bit system;		/* system or user table */
	/*temp_t persistence;	 persistent, global or local temporary */
	int commit_action;  	/* on commit action */
	char *query;		/* views may require some query */
	struct create_tab* next;
}create_tab;

typedef struct create_indx
{
	int ioid;     /* index oid, add for retry create index */
	int toid;
	char iname[1024];
	int type;
	struct create_indx* next;
}create_indx;

typedef struct create_dependency
{
	int doid;     /* dependency oid, add for retry create dependency */
	int depend_id;
	short depend_type;
	struct create_dependency* next;
}create_dependency;

typedef struct create_schema
{
	int soid;     /* schema oid, add for retry create schema */
	char sname[1024];
	int authorization;
	int owner;
	bit system;
	struct create_schema* next;
}create_schema;
typedef struct drop_tab
{
	int toid;     /* table oid, add for retry create table */
	//char tname[1024];
	struct drop_tab* next;
}drop_tab;
typedef struct drop_col
{
	int coid;
	struct drop_col* next;
}drop_col;


typedef struct logaction {
	int type;		/* type of change */
	lng nr;
	int ht;			/* vid(-1),void etc */
	int tt;
	lng id;
	char *name;		/* optional */
	BAT *b;			/* temporary bat with changes */
	BAT *uid;		/* temporary bat with bun positions to update */
} logaction;

/* during the recover process a number of transactions could be active */
typedef struct trans {
	int tid;		/* transaction id */
	int sz;			/* sz of the changes array */
	int nr;			/* nr of changes */
	char gid[TMGIDSIZE];
	logaction *changes;
  
	create_tab *cr_tabs;
	create_clm *cr_cols;
	create_dependency *cr_depds;
	create_indx *cr_indx;
	create_schema *cr_schema;
	drop_tab *dr_tabs;
	drop_col *dr_cols;
	struct trans *tr;
} trans;

typedef int (*preversionfix_fptr)(int oldversion, int newversion);
typedef void (*postversionfix_fptr)(void *lg);
//extern trans *recover_tr ;

typedef struct logger {
	int debug;
	lng changes;
	int version;
	lng id;
	int tid;
	//char *gtid;	 /* global trans id; add for distributed transation */
#if SIZEOF_OID == 8
	/* on 64-bit architecture, read OIDs as 32 bits (for upgrading
	 * oid size) */
	int read32bitoid;
#endif
	char *fn;
	char *dir;
	char *local_dir; /* the directory in which the non-shared log is written */
	int shared; /* a flag to indicate if the logger is a shared on (usually read-only) */
	int dbfarm_role; /* role for the dbram used for the logdir, PERSISTENT by default */
	int local_dbfarm_role; /* role for the dbram used for the logdir, PERSISTENT by default */
	preversionfix_fptr prefuncp;
	postversionfix_fptr postfuncp;
	stream *log;
	lng end;		/* end of pre-allocated blocks for faster f(data)sync */
	/* Store log_bids (int) to circumvent trouble with reference counting */
	BAT *catalog_bid;	/* int bid column */
	BAT *catalog_nme;	/* str name column */
	BAT *dcatalog;		/* deleted from catalog table */
	BAT *seqs_id;		/* int id column */
	BAT *seqs_val;		/* lng value column */
	BAT *dseqs;		/* deleted from seqs table */
	BAT *snapshots_bid;	/* int bid column */
	BAT *snapshots_tid;	/* int tid column */
	BAT *dsnapshots;	/* deleted from snapshots table */
	BAT *freed;		/* snapshots can be created and destroyed,
				   in a single logger transaction.
				   These snapshot bats should be freed
				   directly (on transaction
				   commit). */
	void *buf;
	size_t bufsize;
} logger;

/* Holds logger settings
 * if shared_logdir and shared_drift_threshold are set,
 * as well as if readonly = 1, the instance is assumed to be in slave mode*/
typedef struct logger_settings {
	char *logdir;	/* (the regular) server write-ahead log directory */
	char *shared_logdir;	/* shared write-ahead log directory */
	int	shared_drift_threshold; /* shared write-ahead log drift threshold */
	int keep_persisted_log_files; 	/* a flag if old WAL files should be preserved */
} logger_settings;

#define BATSIZE 0

typedef int log_bid;

/*
 * @+ Sequence numbers
 * The logger also keeps sequence numbers. A sequence needs to store
 * each requested (block) of sequence numbers. This is done using the
 * log_sequence function. The logger_sequence function can be used to
 * return the last logged sequence number. Sequences identifiers
 * should be unique, and 2 are already used. The first LOG_SID is used
 * internally for the log files sequence. The second OBJ_SID is for
 * frontend objects, for example the sql objects have a global
 * sequence counter such that each table, trigger, sequence etc. has a
 * unique number.
 */
/* the sequence identifier for the sequence of log files */
#define LOG_SID	0
/* the sequence identifier for frontend objects */
#define OBJ_SID	1
#define RECOVER_COMMITED 1
#define RECOVER_ABORT 2
#define RETRY_PREPARED 3

gdk_export logger *logger_create(int debug, const char *fn, const char *logdir, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp, int keep_persisted_log_files);
gdk_export logger *logger_create_shared(int debug, const char *fn, const char *logdir, const char *slave_logdir, int version, preversionfix_fptr prefuncp, postversionfix_fptr postfuncp);
gdk_export void logger_destroy(logger *lg);
gdk_export int logger_exit(logger *lg);
gdk_export int logger_restart(logger *lg);
gdk_export int logger_cleanup(logger *lg, int keep_persisted_log_files);
gdk_export lng logger_changes(logger *lg);
gdk_export lng logger_read_last_transaction_id(logger *lg, char *dir, char *logger_file, int role);
gdk_export int logger_sequence(logger *lg, int seq, lng *id);
gdk_export int logger_reload(logger *lg);

/* todo pass the transaction id */
gdk_export int log_bat(logger *lg, BAT *b, const char *n);
gdk_export int log_bat_clear(logger *lg, const char *n);
gdk_export int log_bat_persists(logger *lg, BAT *b, const char *n);
gdk_export int log_bat_transient(logger *lg, const char *n);
gdk_export int log_delta(logger *lg, BAT *uid, BAT *uval, const char *n);

gdk_export int log_tstart(logger *lg);	/* TODO return transaction id */
gdk_export int log_tend(logger *lg);
gdk_export int log_retry_end(logger *lg);
gdk_export int log_abort(logger *lg);
gdk_export int log_retry_abort(logger *lg);
gdk_export int log_prepare(logger *lg); /* write prepare log; add for distributed transation*/

gdk_export int log_sequence(logger *lg, int seq, lng id);

gdk_export log_bid logger_add_bat(logger *lg, BAT *b, const char *name);
gdk_export void logger_del_bat(logger *lg, log_bid bid);
gdk_export log_bid logger_find_bat(logger *lg, const char *name);

typedef int (*geomcatalogfix_fptr)(void *, int);
gdk_export void geomcatalogfix_set(geomcatalogfix_fptr);
gdk_export geomcatalogfix_fptr geomcatalogfix_get(void);

typedef str (*geomsqlfix_fptr)(int);
gdk_export void geomsqlfix_set(geomsqlfix_fptr);
gdk_export geomsqlfix_fptr geomsqlfix_get(void);

gdk_export void geomversion_set(void);
gdk_export int geomversion_get(void);

gdk_export int logger_readlog2(logger *lg, char *filename);/* add for logReader tool*/
gdk_export trans *recover_tr;
gdk_export trans *tr_destroy(trans *tr);
gdk_export void la_apply(logger *lg, logaction *c);
gdk_export void la_destroy(logaction *c);
#endif /*_LOGGER_H_*/
