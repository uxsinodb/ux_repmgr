/*
 * dbutils.h
 *
 * Portions Copyright (c) 2016-2022, Beijing Uxsino Software Limited, Co.
 * Copyright (c) 2009-2020, UXDB Software Co.,Ltd.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#ifndef _REPMGR_DBUTILS_H_
#define _REPMGR_DBUTILS_H_

#include "access/timeline.h"
#include "access/xlogdefs.h"
#include "uxsqlexpbuffer.h"
#include "portability/instr_time.h"

#include "configfile.h"
#include "strutil.h"
#include "voting.h"

#define REPMGR_NODES_COLUMNS \
	"n.node_id, " \
	"n.type, " \
	"n.upstream_node_id, " \
	"n.node_name,  " \
	"n.conninfo, " \
	"n.repluser, " \
	"n.slot_name, " \
	"n.location, " \
	"n.priority, " \
	"n.active, " \
	"n.config_file, " \
	"'' AS upstream_node_name, " \
	"n.uxdb_passwd, " \
	"n.root_passwd, " \
	"NULL AS attached "

#define REPMGR_NODES_COLUMNS_WITH_UPSTREAM \
	"n.node_id, " \
	"n.type, " \
	"n.upstream_node_id, " \
	"n.node_name, " \
	"n.conninfo, " \
	"n.repluser, " \
	"n.slot_name, " \
	"n.location, " \
	"n.priority, " \
	"n.active, "\
	"n.config_file, " \
	"un.node_name AS upstream_node_name, " \
	"n.uxdb_passwd, " \
	"n.root_passwd, " \
	"NULL AS attached "


#define ERRBUFF_SIZE 512

typedef enum
{
	UNKNOWN = 0,
	PRIMARY,
	STANDBY,
	WITNESS
} t_server_type;

typedef enum
{
	REPMGR_INSTALLED = 0,
	REPMGR_OLD_VERSION_INSTALLED,
	REPMGR_AVAILABLE,
	REPMGR_UNAVAILABLE,
	REPMGR_UNKNOWN
} ExtensionStatus;

typedef enum
{
	RECTYPE_UNKNOWN = -1,
	RECTYPE_PRIMARY,
	RECTYPE_STANDBY
} RecoveryType;

typedef enum
{
	RECORD_ERROR = -1,
	RECORD_FOUND,
	RECORD_NOT_FOUND
} RecordStatus;

typedef enum
{
	MS_NORMAL = 0,
	MS_DEGRADED = 1
} MonitoringState;

typedef enum
{
	NODE_STATUS_UNKNOWN = -1,
	NODE_STATUS_UP,
	NODE_STATUS_SHUTTING_DOWN,
	NODE_STATUS_DOWN,
	NODE_STATUS_UNCLEAN_SHUTDOWN,
	NODE_STATUS_REJECTED
} NodeStatus;

typedef enum
{
	CONN_UNKNOWN = -1,
	CONN_OK,
	CONN_BAD,
	CONN_ERROR
} ConnectionStatus;

typedef enum
{
	/* unable to query "ux_stat_replication" or other error */
	NODE_ATTACHED_UNKNOWN = -1,
	/* node has record in "ux_stat_replication" and state is not "streaming" */
	NODE_ATTACHED,
	/* node has record in "ux_stat_replication" but state is not "streaming" */
	NODE_NOT_ATTACHED,
	/* node has no record in "ux_stat_replication" */
	NODE_DETACHED
} NodeAttached;

typedef enum
{
	SLOT_UNKNOWN = -1,
	SLOT_NOT_FOUND,
	SLOT_NOT_PHYSICAL,
	SLOT_INACTIVE,
	SLOT_ACTIVE
} ReplSlotStatus;

typedef enum
{
	BACKUP_STATE_UNKNOWN = -1,
	BACKUP_STATE_IN_BACKUP,
	BACKUP_STATE_NO_BACKUP
} BackupState;



/*
 * Struct to store extension version information
 */

typedef struct s_extension_versions {
	char		default_version[8];
	int			default_version_num;
	char		installed_version[8];
	int			installed_version_num;
} t_extension_versions;

#define T_EXTENSION_VERSIONS_INITIALIZER { \
	"", \
	UNKNOWN_SERVER_VERSION_NUM, \
	"", \
	UNKNOWN_SERVER_VERSION_NUM \
}


typedef struct
{
	char		current_timestamp[MAXLEN];
	bool		in_recovery;
	TimeLineID	timeline_id;
	char		timeline_id_str[MAXLEN];
	XLogRecPtr	last_wal_receive_lsn;
	XLogRecPtr	last_wal_replay_lsn;
	char		last_xact_replay_timestamp[MAXLEN];
	int			replication_lag_time;
	bool		receiving_streamed_wal;
	bool		wal_replay_paused;
	int			upstream_last_seen;
	int			upstream_node_id;
} ReplInfo;

/*
 * Struct to store node information.
 *
 * The first section represents the contents of the "repmgr.nodes"
 * table; subsequent section contain information collated in
 * various contexts.
 */
typedef struct s_node_info
{
	/* contents of "repmgr.nodes" */
	int			node_id;
	int			upstream_node_id;
	t_server_type type;
	char		node_name[NAMEDATALEN];
	char		upstream_node_name[NAMEDATALEN];
	char		conninfo[MAXLEN];
	char		repluser[NAMEDATALEN];
	char		location[MAXLEN];
	int			priority;
	bool		active;
	char		slot_name[MAXLEN];
	char		config_file[MAXUXPATH];
	char        virtual_ip[MAXUXPATH];   //uxdb
	char        network_card[MAXUXPATH]; //uxdb
	/* user passwd */
	char        root_passwd[MAXUXPATH];
	char        uxdb_passwd[MAXUXPATH];
	/* used during failover to track node status */
	XLogRecPtr	last_wal_receive_lsn;
	NodeStatus	node_status;
	RecoveryType recovery_type;
	MonitoringState monitoring_state;
	UXconn	   *conn;
	/* for ad-hoc use e.g. when working with a list of nodes */
	char		details[MAXLEN];
	bool		reachable;
	NodeAttached attached;
	/* various statistics */
	int			max_wal_senders;
	int			attached_wal_receivers;
	int			max_replication_slots;
	int			total_replication_slots;
	int			active_replication_slots;
	int			inactive_replication_slots;
	/* replication info */
	ReplInfo   *replication_info;
	/* uxdb: record counts that failed connect to node */
	int         failed_connect_times;
} t_node_info;


#define T_NODE_INFO_INITIALIZER { \
   	/* contents of "repmgr.nodes" */ \
	NODE_NOT_FOUND, \
	NO_UPSTREAM_NODE, \
	UNKNOWN, \
	"", \
	"", \
	"", \
	"", \
	DEFAULT_LOCATION, \
	DEFAULT_PRIORITY, \
	true, \
	"", \
	"", \
	/* uxdb: virtual default value */ \
	"", \
	"", \
	"", \
	"", \
	/* used during failover to track node status */ \
	InvalidXLogRecPtr, \
	NODE_STATUS_UNKNOWN, \
	RECTYPE_UNKNOWN,  \
	MS_NORMAL, \
	NULL, \
	/* for ad-hoc use e.g. when working with a list of nodes */ \
	"", true, true,	\
	/* various statistics */ \
	-1, -1, -1, -1, -1, -1,	\
	NULL \
}


/* structs to store a list of repmgr node records */
typedef struct NodeInfoListCell
{
	struct NodeInfoListCell *next;
	t_node_info *node_info;
	ReplInfo  replinfo; //uxdb
} NodeInfoListCell;

typedef struct NodeInfoList
{
	NodeInfoListCell *head;
	NodeInfoListCell *tail;
	int			node_count;
} NodeInfoList;

#define T_NODE_INFO_LIST_INITIALIZER { \
	NULL, \
	NULL, \
	0 \
}

typedef struct s_event_info
{
	char	   *node_name;
	char	   *conninfo_str;
	int			node_id;
} t_event_info;

#define T_EVENT_INFO_INITIALIZER { \
	NULL, \
	NULL, \
	UNKNOWN_NODE_ID \
}


/*
 * Struct to store list of conninfo keywords and values
 */
typedef struct
{
	int			size;
	char	  **keywords;
	char	  **values;
} t_conninfo_param_list;

#define T_CONNINFO_PARAM_LIST_INITIALIZER { \
	0, \
	NULL, \
	NULL, \
}

/*
 * Struct to store replication slot information
 */
typedef struct s_replication_slot
{
	char		slot_name[MAXLEN];
	char		slot_type[MAXLEN];
	bool		active;
} t_replication_slot;

#define T_REPLICATION_SLOT_INITIALIZER { "", "", false }


typedef struct s_connection_user
{
	char		username[MAXLEN];
	bool		is_superuser;
} t_connection_user;

#define T_CONNECTION_USER_INITIALIZER { "", false }


typedef struct
{
	char		filepath[MAXUXPATH];
	char		filename[MAXUXPATH];
	bool		in_data_directory;
} t_configfile_info;

#define T_CONFIGFILE_INFO_INITIALIZER { "", "", false }


typedef struct
{
	int			size;
	int			entries;
	t_configfile_info **files;
} t_configfile_list;

#define T_CONFIGFILE_LIST_INITIALIZER { 0, 0, NULL }


typedef struct
{
	uint64		system_identifier;
	TimeLineID	timeline;
	XLogRecPtr	xlogpos;
} t_system_identification;

#define T_SYSTEM_IDENTIFICATION_INITIALIZER { \
	UNKNOWN_SYSTEM_IDENTIFIER, \
	UNKNOWN_TIMELINE_ID, \
	InvalidXLogRecPtr \
}


typedef struct RepmgrdInfo {
	int node_id;
	int pid;
	char pid_text[MAXLEN];
	char pid_file[MAXLEN];
	bool ux_running;
	char ux_running_text[MAXLEN];
	RecoveryType recovery_type;
	bool running;
	char repmgrd_running[MAXLEN];
	bool paused;
	bool wal_paused_pending_wal;
	int  upstream_last_seen;
	char upstream_last_seen_text[MAXLEN];
} RepmgrdInfo;


/* macros */

#define is_streaming_replication(x) (x == PRIMARY || x == STANDBY)
#define format_lsn(x) (uint32) (x >> 32), (uint32) x

/* utility functions */

XLogRecPtr	parse_lsn(const char *str);
bool		atobool(const char *value);

/* connection functions */
UXconn	   *establish_db_connection(const char *conninfo,
						const bool exit_on_error);
UXconn	   *establish_db_connection_quiet(const char *conninfo);
UXconn	   *establish_db_connection_by_params(t_conninfo_param_list *param_list,
								  const bool exit_on_error);
UXconn	   *establish_db_connection_with_replacement_param(const char *conninfo,
														   const char *param,
														   const char *value,
														   const bool exit_on_error);
UXconn	   *establish_replication_connection_from_conn(UXconn *conn, const char *repluser);
UXconn	   *establish_replication_connection_from_conninfo(const char *conninfo, const char *repluser);

UXconn	   *establish_primary_db_connection(UXconn *conn,
								const bool exit_on_error);
UXconn	   *get_primary_connection(UXconn *standby_conn, int *primary_id, char *primary_conninfo_out);
UXconn	   *get_primary_connection_quiet(UXconn *standby_conn, int *primary_id, char *primary_conninfo_out);
UXconn	   *duplicate_connection(UXconn *conn, const char *user, bool replication);

void		close_connection(UXconn **conn);

/* conninfo manipulation functions */
bool		get_conninfo_value(const char *conninfo, const char *keyword, char *output);
bool		get_conninfo_default_value(const char *param, char *output, int maxlen);
void		initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults);
void		free_conninfo_params(t_conninfo_param_list *param_list);
void		copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list);
void		conn_to_param_list(UXconn *conn, t_conninfo_param_list *param_list);
void		param_set(t_conninfo_param_list *param_list, const char *param, const char *value);
void		param_set_ine(t_conninfo_param_list *param_list, const char *param, const char *value);
char	   *param_get(t_conninfo_param_list *param_list, const char *param);
bool		validate_conninfo_string(const char *conninfo_str, char **errmsg);
bool		parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char **errmsg, bool ignore_local_params);
char	   *param_list_to_string(t_conninfo_param_list *param_list);
char	   *normalize_conninfo_string(const char *conninfo_str);
bool		has_passfile(void);


/* transaction functions */
bool		begin_transaction(UXconn *conn);
bool		commit_transaction(UXconn *conn);
bool		rollback_transaction(UXconn *conn);

/* GUC manipulation functions */
bool		set_config(UXconn *conn, const char *config_param, const char *config_value);
bool		set_config_bool(UXconn *conn, const char *config_param, bool state);
int		    guc_set(UXconn *conn, const char *parameter, const char *op, const char *value);
bool		get_ux_setting(UXconn *conn, const char *setting, char *output);
bool		get_ux_setting_bool(UXconn *conn, const char *setting, bool *output);
bool		get_ux_setting_int(UXconn *conn, const char *setting, int *output);
bool		alter_system_int(UXconn *conn, const char *name, int value);
bool		ux_reload_conf(UXconn *conn);
bool		alter_system_str(UXconn *conn, const char *name, const char *value);

/* server information functions */
bool		get_cluster_size(UXconn *conn, char *size);
int			get_server_version(UXconn *conn, char *server_version_buf);

RecoveryType get_recovery_type(UXconn *conn);
int			get_primary_node_id(UXconn *conn);
int			get_ready_archive_files(UXconn *conn, const char *data_directory);
bool		identify_system(UXconn *repl_conn, t_system_identification *identification);
uint64		system_identifier(UXconn *conn);
TimeLineHistoryEntry *get_timeline_history(UXconn *repl_conn, TimeLineID tli);
pid_t		get_wal_receiver_pid(UXconn *conn);

/* user/role information functions */
bool		can_execute_ux_promote(UXconn *conn);
bool		can_disable_walsender(UXconn *conn);
bool		connection_has_ux_monitor_role(UXconn *conn, const char *subrole);
bool		is_replication_role(UXconn *conn, char *rolname);
bool		is_superuser_connection(UXconn *conn, t_connection_user *userinfo);

/* repmgrd shared memory functions */
bool		repmgrd_set_local_node_id(UXconn *conn, int local_node_id);
int			repmgrd_get_local_node_id(UXconn *conn);
bool		repmgrd_check_local_node_id(UXconn *conn);
BackupState	server_in_exclusive_backup_mode(UXconn *conn);
void		repmgrd_set_pid(UXconn *conn, pid_t repmgrd_pid, const char *pidfile);
pid_t		repmgrd_get_pid(UXconn *conn);
bool		repmgrd_is_running(UXconn *conn);
bool		repmgrd_is_paused(UXconn *conn);
bool		repmgrd_pause(UXconn *conn, bool pause);
int			repmgrd_get_upstream_node_id(UXconn *conn);
bool		repmgrd_set_upstream_node_id(UXconn *conn, int node_id);

/* extension functions */
ExtensionStatus get_repmgr_extension_status(UXconn *conn, t_extension_versions *extversions);

/* node management functions */
void		checkpoint(UXconn *conn);
bool		vacuum_table(UXconn *conn, const char *table);
bool		promote_standby(UXconn *conn, bool wait, int wait_seconds);
bool		resume_wal_replay(UXconn *conn);

/* node record functions */
t_server_type parse_node_type(const char *type);
const char *get_node_type_string(t_server_type type);

RecordStatus get_node_record(UXconn *conn, int node_id, t_node_info *node_info);
RecordStatus refresh_node_record(UXconn *conn, int node_id, t_node_info *node_info);

RecordStatus get_node_record_with_upstream(UXconn *conn, int node_id, t_node_info *node_info);

RecordStatus get_node_record_by_name(UXconn *conn, const char *node_name, t_node_info *node_info);
t_node_info *get_node_record_pointer(UXconn *conn, int node_id);

bool		get_local_node_record(UXconn *conn, int node_id, t_node_info *node_info);
bool		get_primary_node_record(UXconn *conn, t_node_info *node_info);

void        ux_get_all_node_records(UXconn *conn, NodeInfoList *node_list); //uxdb added
bool		get_all_node_records(UXconn *conn, NodeInfoList *node_list);
bool		get_all_nodes_count(UXconn *conn, int *count);
void		get_downstream_node_records(UXconn *conn, int node_id, NodeInfoList *nodes);
void		get_active_sibling_node_records(UXconn *conn, int node_id, int upstream_node_id, NodeInfoList *node_list);
bool		get_child_nodes(UXconn *conn, int node_id, NodeInfoList *node_list);
void		get_node_records_by_priority(UXconn *conn, NodeInfoList *node_list);
bool		get_all_node_records_with_upstream(UXconn *conn, NodeInfoList *node_list);
bool		get_downstream_nodes_with_missing_slot(UXconn *conn, int this_node_id, NodeInfoList *noede_list);

bool		create_node_record(UXconn *conn, char *repmgr_action, t_node_info *node_info);
bool		update_node_record(UXconn *conn, char *repmgr_action, t_node_info *node_info);
bool		delete_node_record(UXconn *conn, int node);
bool		truncate_node_records(UXconn *conn);

bool		update_node_record_set_active(UXconn *conn, int this_node_id, bool active);
bool		update_node_record_set_primary(UXconn *conn, int this_node_id);
bool		update_node_record_set_active_standby(UXconn *conn, int this_node_id);
bool		update_node_record_set_upstream(UXconn *conn, int this_node_id, int new_upstream_node_id);
bool		update_node_record_status(UXconn *conn, int this_node_id, char *type, int upstream_node_id, bool active);
bool		update_node_record_conn_priority(UXconn *conn, t_configuration_options *options);
bool		update_node_record_slot_name(UXconn *primary_conn, int node_id, char *slot_name);

bool		witness_copy_node_records(UXconn *primary_conn, UXconn *witness_conn);

void		clear_node_info_list(NodeInfoList *nodes);

/* UXsinoDB configuration file location functions */
bool		get_datadir_configuration_files(UXconn *conn, KeyValueList *list);
bool		get_configuration_file_locations(UXconn *conn, t_configfile_list *list);
void		config_file_list_init(t_configfile_list *list, int max_size);
void		config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir);

/* event functions */
bool		create_event_record(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool		create_event_notification(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details);
bool		create_event_notification_extended(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info);
UXresult   *get_event_records(UXconn *conn, int node_id, const char *node_name, const char *event, bool all, int limit);

/* replication slot functions */
void		create_slot_name(char *slot_name, int node_id);

bool		create_replication_slot_sql(UXconn *conn, char *slot_name, UXSQLExpBufferData *error_msg);
bool		create_replication_slot_replprot(UXconn *conn, UXconn *repl_conn, char *slot_name, UXSQLExpBufferData *error_msg);
bool		drop_replication_slot_sql(UXconn *conn, char *slot_name);
bool		drop_replication_slot_replprot(UXconn *repl_conn, char *slot_name);

RecordStatus get_slot_record(UXconn *conn, char *slot_name, t_replication_slot *record);
int			get_free_replication_slot_count(UXconn *conn, int *max_replication_slots);
int			get_inactive_replication_slots(UXconn *conn, KeyValueList *list);

/* tablespace functions */
bool		get_tablespace_name_by_location(UXconn *conn, const char *location, char *name);

/* asynchronous query functions */
bool		cancel_query(UXconn *conn, int timeout);
int			wait_connection_availability(UXconn *conn, int timeout);

/* node availability functions */
bool		is_server_available(const char *conninfo);
bool		is_server_available_quiet(const char *conninfo);
bool		is_server_available_params(t_conninfo_param_list *param_list);
ExecStatusType	connection_ping(UXconn *conn);
ExecStatusType	connection_ping_reconnect(UXconn *conn);

/* monitoring functions  */
void
add_monitoring_record(UXconn *primary_conn,
					  UXconn *local_conn,
					  int primary_node_id,
					  int local_node_id,
					  char *monitor_standby_timestamp,
					  XLogRecPtr primary_last_wal_location,
					  XLogRecPtr last_wal_receive_lsn,
					  char *last_xact_replay_timestamp,
					  long long unsigned int replication_lag_bytes,
					  long long unsigned int apply_lag_bytes
);

int			get_number_of_monitoring_records_to_delete(UXconn *primary_conn, int keep_history, int node_id);
bool		delete_monitoring_records(UXconn *primary_conn, int keep_history, int node_id);



/* node voting functions */
void		initialize_voting_term(UXconn *conn);
int			get_current_term(UXconn *conn);
void		increment_current_term(UXconn *conn);
bool		announce_candidature(UXconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term);
void		notify_follow_primary(UXconn *conn, int primary_node_id);
bool		get_new_primary(UXconn *conn, int *primary_node_id);
void		reset_voting_status(UXconn *conn);

/* replication status functions */
/* add by douwen for bug #179168, 2023/3/16,reviewer:huyn */
char		*check_repliction_sync_async(UXconn *conn); 
XLogRecPtr	get_primary_current_lsn(UXconn *conn);
XLogRecPtr	get_node_current_lsn(UXconn *conn);
XLogRecPtr	get_last_wal_receive_location(UXconn *conn);
void		init_replication_info(ReplInfo *replication_info);
bool		get_replication_info(UXconn *conn, t_server_type node_type, ReplInfo *replication_info);
int			get_replication_lag_seconds(UXconn *conn);
TimeLineID	get_node_timeline(UXconn *conn, char *timeline_id_str);
void		get_node_replication_stats(UXconn *conn, t_node_info *node_info);
NodeAttached is_downstream_node_attached(UXconn *conn, char *node_name, char **node_state);
NodeAttached is_downstream_node_attached_quiet(UXconn *conn, char *node_name, char **node_state);
void		set_upstream_last_seen(UXconn *conn, int upstream_node_id);
int			get_upstream_last_seen(UXconn *conn, t_server_type node_type);

bool		is_wal_replay_paused(UXconn *conn, bool check_pending_wal);

/* repmgrd status functions */
CheckStatus	get_repmgrd_status(UXconn *conn);

/* miscellaneous debugging functions */
const char *print_node_status(NodeStatus node_status);
const char *print_uxsqlping_status(UXPing ping_status);

/*uxdb:*/
void get_ux_size_pretty(UXconn *conn, long long unsigned int lag_bytes, char *lag_str);
/* uxdb: VIP manager functions */
bool bind_virtual_ip(const char *vip, const char *network_card);
bool unbind_virtual_ip(const char *vip, const char *network_card);
bool check_vip_conf(const char *vip, const char *network_card);
bool get_virtual_ip(UXconn *conn, int primary_id, char *virtual_ip);
bool get_network_card(UXconn *conn, int primary_id, char *network_card);
void new_primary_execute_checkpoint(UXconn *conn);

#endif							/* _REPMGR_DBUTILS_H_ */
