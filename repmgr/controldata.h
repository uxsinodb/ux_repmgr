/*
 * controldata.h
 * Portions Copyright (c) 2016-2022, Beijing Uxsino Software Limited, Co.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

#ifndef _CONTROLDATA_H_
#define _CONTROLDATA_H_

#include "uxdb_fe.h"
#include "catalog/ux_control.h"

#define MAX_VERSION_STRING 24
/* add by dengwei for #158252、#160452 , 2022/08/24 reveiwer:wang_bocai */
#define UX_PROMPT_VERSION_LEN			16

/* Nonce key length, see below */
#define MOCK_AUTH_NONCE_LEN		32
/* Add by duankun for #151794, 2022/12/30 */
#define DATABASEKEY_SIZE		32
/*
 * A simplified representation of ux_control containing only those fields
 * required by repmgr.
 */
typedef struct
{
	bool		control_file_processed;
	uint64		system_identifier;
	DBState		state;
	XLogRecPtr	checkPoint;
	uint32		data_checksum_version;
	TimeLineID	timeline;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	minRecoveryPoint;
} ControlFileInfo;


typedef struct CheckPoint94
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	bool		fullPageWrites; /* current full_page_writes */
	uint32		nextXidEpoch;	/* higher-order bits of nextXid */
	TransactionId nextXid;		/* next free XID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */
	MultiXactId oldestMulti;	/* cluster-wide minimum datminmxid */
	Oid			oldestMultiDB;	/* database with minimum datminmxid */
	ux_time_t	time;			/* time stamp of checkpoint */

	TransactionId oldestActiveXid;
} CheckPoint94;


/* Same for 9.5, 9.6, 10, 11 */
typedef struct CheckPoint95
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	bool		fullPageWrites; /* current full_page_writes */
	uint32		nextXidEpoch;	/* higher-order bits of nextXid */
	TransactionId nextXid;		/* next free XID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */
	MultiXactId oldestMulti;	/* cluster-wide minimum datminmxid */
	Oid			oldestMultiDB;	/* database with minimum datminmxid */
	ux_time_t	time;			/* time stamp of checkpoint */
	TransactionId oldestCommitTsXid;	/* oldest Xid with valid commit
										 * timestamp */
	TransactionId newestCommitTsXid;	/* newest Xid with valid commit
										 * timestamp */

	TransactionId oldestActiveXid;
} CheckPoint95;

/*
 * Following fields removed in UXsinoDB 12;
 *
 *   uint32 nextXidEpoch;
 *   TransactionId nextXid;
 *
 * and replaced by:
 *
 *   FullTransactionId nextFullXid;
 */

typedef struct CheckPoint12
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	bool		fullPageWrites; /* current full_page_writes */
	FullTransactionId nextFullXid;	/* next free full transaction ID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */
	MultiXactId oldestMulti;	/* cluster-wide minimum datminmxid */
	Oid			oldestMultiDB;	/* database with minimum datminmxid */
	ux_time_t	time;			/* time stamp of checkpoint */
	TransactionId oldestCommitTsXid;	/* oldest Xid with valid commit
										 * timestamp */
	TransactionId newestCommitTsXid;	/* newest Xid with valid commit
										 * timestamp */

	/*
	 * Oldest XID still running. This is only needed to initialize hot standby
	 * mode from an online checkpoint, so we only bother calculating this for
	 * online checkpoints and only when wal_level is replica. Otherwise it's
	 * set to InvalidTransactionId.
	 */
	TransactionId oldestActiveXid;
} CheckPoint12;


typedef struct ControlFileData94
{
	uint64		system_identifier;

	uint32		ux_control_version;		/* UX_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	DBState		state;			/* see enum above */
	ux_time_t	time;			/* time stamp of last ux_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */
	XLogRecPtr	prevCheckPoint; /* previous check point record ptr */

	CheckPoint94	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_prepared_xacts;
	int			max_locks_per_xact;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in ux_largeobject */

	bool		enableIntTimes; /* int64 storage enabled? */

	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	/* Are data pages protected by checksums? Zero if no checksum version */
	uint32		data_checksum_version;

} ControlFileData94;



/*
 * Following field added since 9.4:
 *
 *	bool		track_commit_timestamp;
 *
 * Unchanged in 9.6
 *
 * In 10, following field appended *after* "data_checksum_version":
 *
 *	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];
 *
 * (but we don't care about that)
 */

typedef struct ControlFileData95
{
	uint64		system_identifier;

	uint32		ux_control_version;		/* UX_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	DBState		state;			/* see enum above */
	ux_time_t	time;			/* time stamp of last ux_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */
	XLogRecPtr	prevCheckPoint; /* previous check point record ptr */

	CheckPoint95	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in ux_largeobject */

	bool		enableIntTimes; /* int64 storage enabled? */

	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	uint32		data_checksum_version;

} ControlFileData95;

/*
 * Following field removed in 11:
 *
 *  XLogRecPtr	prevCheckPoint;
 *
 * In 10, following field appended *after* "data_checksum_version":
 *
 * 	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];
 *
 * (but we don't care about that)
 */

typedef struct ControlFileData11
{
	uint64		system_identifier;

	uint32		ux_control_version;		/* UX_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	DBState		state;			/* see enum above */
	ux_time_t	time;			/* time stamp of last ux_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */

	CheckPoint95	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in ux_largeobject */

	bool		enableIntTimes; /* int64 storage enabled? */

	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	uint32		data_checksum_version;

} ControlFileData11;

/*
 * Following field added in Ux12:
 *
 *   int max_wal_senders;
 */

typedef struct ControlFileData12
{
	uint64		system_identifier;

	uint32		ux_control_version; /* UX_CONTROL_VERSION */
	uint32		catalog_version_no; /* see catversion.h */

	DBState		state;			/* see enum above */
	ux_time_t	time;			/* time stamp of last ux_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */

	CheckPoint12	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_wal_senders;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;

	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */

	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in ux_largeobject */

	bool		float4ByVal;	/* float4 pass-by-value? */
	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	uint32		data_checksum_version;

	/* BEGIN add by huyn for #160452 , 2022/09/8 reveiwer:wangbocai */
	/* remove ControlFileData struct member variable to here */
	/*
	 * Random nonce, used in authentication requests that need to proceed
	 * based on values that are cluster-unique, like a SASL exchange that
	 * failed at an early stage.
	 */
	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];

	/* add by zhaoqigui for #91146 at 2020.10.30 */
	/* Case sensitive flag */
	bool		ignore_cased; 

	bool		running_mode_control; 

	/* add by dengwei for #158252、#160452 , 2022/08/24 reveiwer:wang_bocai */
	char		ux_prompt_control_version[UX_PROMPT_VERSION_LEN]; /* uxdb check version */

	/* Add by yuangz for #112072/#122598, reviewer:tanx */
	bool		running_mode_security;
	
	/* Add by duankun for #151794, 2022/12/30 */
	bool		full_crypto;	/* 全库加密状态：打开或者关闭 */
	bool		wal_crypto_stat;	/* wal加密功能状态:打开/关闭 */
	char		database_crypto_key[DATABASEKEY_SIZE];	/* database加密密钥 */
	bool		audit_crypto_stat;		/* 审计日志加密状态:打开/关闭 */

	bool		checksum_hmac_sm3;	/*checksum算法是否使用hmac-sm3*/

	/* Add by tianjy for #127833, 2021/12/23 reviewer:wenz */
	bool		all_security_features_loaded;	/* 全部安全功能已加载过的标志 */
	
	/* CRC of all above ... MUST BE LAST! */
	ux_crc32c	crc;

} ControlFileData12;

extern int get_ux_version(const char *data_directory, char *version_string);
extern bool get_db_state(const char *data_directory, DBState *state);
extern const char *describe_db_state(DBState state);
extern int	get_data_checksum_version(const char *data_directory);
extern uint64 get_system_identifier(const char *data_directory);
extern XLogRecPtr get_latest_checkpoint_location(const char *data_directory);
extern TimeLineID get_timeline(const char *data_directory);
extern TimeLineID get_min_recovery_end_timeline(const char *data_directory);
extern XLogRecPtr get_min_recovery_location(const char *data_directory);

#endif							/* _CONTROLDATA_H_ */
