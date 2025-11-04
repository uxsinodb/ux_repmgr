/*
 * repmgr-client-global.h
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
 */

#ifndef _REPMGR_CLIENT_GLOBAL_H_
#define _REPMGR_CLIENT_GLOBAL_H_

#include "configfile.h"

/* values for --copy-external-config-files */
#define CONFIG_FILE_SAMEPATH 1
#define CONFIG_FILE_UXDATA 2

/* default value for "cluster event --limit"*/
#define CLUSTER_EVENT_LIMIT 20



typedef struct
{
	/* configuration metadata */
	bool		conninfo_provided;
	bool		connection_param_provided;
	bool		host_param_provided;
	bool		limit_provided;
	bool		wait_provided;

	/* general configuration options */
	char		config_file[MAXUXPATH];
	bool		dry_run;
	bool		force;
	char		ux_bindir[MAXLEN];	/* overrides setting in repmgr.conf */
	int			wait;
	bool		no_wait;
	bool		compact;
	bool		detail;
	bool		dump_config;

	/* logging options */
	char		log_level[MAXLEN];	/* overrides setting in repmgr.conf */
	bool		log_to_file;
	bool		quiet;
	bool		terse;
	bool		verbose;

	/* output options */
	bool		csv;
	bool		nagios;
	bool		optformat;

	/* standard connection options */
	char		dbname[MAXLEN];
	char		host[MAXLEN];
	char		username[MAXLEN];
	char		port[MAXLEN];

	/* other connection options */
	char		remote_user[MAXLEN];
	char		superuser[MAXLEN];

	/* general node options */
	int			node_id;
	char		node_name[NAMEDATALEN];
	char		data_dir[MAXUXPATH];
	int			remote_node_id;

	/* "standby clone" options */
	bool		copy_external_config_files;
	int			copy_external_config_files_destination;
	bool		fast_checkpoint;
	bool		rsync_only;
	bool		no_upstream_connection;
	char		recovery_min_apply_delay[MAXLEN];	/* overrides setting in repmgr.conf */
	char		replication_user[MAXLEN];
	char		upstream_conninfo[MAXLEN];
	bool		without_barman;
	bool		replication_conf_only;
	bool		verify_backup;

	/* "standby clone"/"standby follow" options */
	int			upstream_node_id;

	/* "standby register" options */
	bool		wait_register_sync;
	int			wait_register_sync_seconds;
	int			wait_start;

	/* "standby switchover" options */
	bool		always_promote;
	bool		force_rewind_used;
	char		force_rewind_path[MAXUXPATH];
	bool		siblings_follow;
	bool		repmgrd_no_pause;
	bool		repmgrd_force_unpause;

	/* "node status" options */
	bool		is_shutdown_cleanly;

	/* "node check" options */
	bool		archive_ready;
	bool		downstream;
	bool		upstream;
	bool		replication_lag;
	bool		role;
	bool		slots;
	bool		missing_slots;
	bool		has_passfile;
	bool		replication_connection;
	bool		repmgrd;
	bool		data_directory_config;
	bool		replication_config_owner;
	bool		db_connection;

	/* "node rejoin" options */
	char		config_files[MAXLEN];

	/* "node service" options */
	char		action[MAXLEN];
	bool		check;
	bool		list_actions;
	bool		checkpoint;

	/* "cluster event" options */
	bool		all;
	char		event[MAXLEN];
	int			limit;

	/* "cluster cleanup" options */
	int			keep_history;

	/* following options for internal use */
	char		config_archive_dir[MAXUXPATH];
	OutputMode	output_mode; /* set through provision of --csv, --nagios or --optformat */
	bool		disable_wal_receiver;
	bool		enable_wal_receiver;
} t_runtime_options;

#define T_RUNTIME_OPTIONS_INITIALIZER { \
		/* configuration metadata */ \
		false, false, false, false, false,	\
		/* general configuration options */	\
		"", false, false, "", -1, false, false, false, false, \
		/* logging options */ \
		"", false, false, false, false,	\
		/* output options */ \
		false, false, false,  \
		/* database connection options */ \
		"", "", "",	"", \
		/* other connection options */ \
		"",	"", \
		/* general node options */ \
		UNKNOWN_NODE_ID, "", "", UNKNOWN_NODE_ID, \
		/* "standby clone" options */ \
		false, CONFIG_FILE_SAMEPATH, false, false, false, "", "", "", \
		false, false, false, \
		/* "standby clone"/"standby follow" options */ \
		NO_UPSTREAM_NODE, \
		/* "standby register" options */ \
		false, -1, DEFAULT_WAIT_START,   \
		/* "standby switchover" options */ \
		false, false, "", false, false, false,	\
		/* "node status" options */ \
		false, \
		/* "node check" options */ \
		false, false, false, false, false, false, false, false,	false, false, false, false, false, \
		/* "node rejoin" options */ \
		"", \
		/* "node service" options */ \
		"", false, false, false,  \
		/* "cluster event" options */ \
		false, "", CLUSTER_EVENT_LIMIT,	\
		/* "cluster cleanup" options */ \
		0, \
		/* following options for internal use */ \
		"/tmp", OM_TEXT, false, false \
}


typedef enum
{
	barman,
	ux_basebackup,
	ux_backupapi
} standy_clone_mode;

typedef enum
{
	ACTION_UNKNOWN = -1,
	ACTION_NONE,
	ACTION_START,
	ACTION_STOP,
	ACTION_STOP_WAIT,
	ACTION_RESTART,
	ACTION_RELOAD,
	ACTION_PROMOTE
} t_server_action;


typedef enum
{
	USER_TYPE_UNKNOWN = -1,
	REPMGR_USER,
	REPLICATION_USER_OPT,
	REPLICATION_USER_NODE,
	SUPERUSER
} t_user_type;

typedef enum
{
	JOIN_UNKNOWN = -1,
	JOIN_SUCCESS,
	JOIN_COMMAND_FAIL,
	JOIN_FAIL_NO_PING,
	JOIN_FAIL_NO_REPLICATION
} standy_join_status;

typedef enum
{
	REMOTE_ERROR_UNKNOWN = -1,
	REMOTE_ERROR_NONE,
	REMOTE_ERROR_DB_CONNECTION,
	REMOTE_ERROR_CONNINFO_PARSE
} t_remote_error_type;

typedef struct ColHeader
{
	char		title[MAXLEN];
	int			max_length;
	int			cur_length;
	bool		display;
} ColHeader;



/* globally available configuration structures */
extern t_runtime_options runtime_options;
extern t_conninfo_param_list source_conninfo;
extern t_node_info target_node_info;

/* global variables */
extern bool config_file_required;
extern char ux_bindir[MAXLEN];

/* global functions */
extern int	check_server_version(UXconn *conn, char *server_type, bool exit_on_error, char *server_version_string);
extern bool create_repmgr_extension(UXconn *conn);
extern int	test_ssh_connection(char *host, char *remote_user);

extern standy_clone_mode get_standby_clone_mode(void);

extern int copy_remote_files(char *host, char *remote_user, char *remote_path,
				  char *local_path, bool is_directory, int server_version_num);

extern void print_error_list(ItemList *error_list, int log_level);

extern void make_ux_path(UXSQLExpBufferData *buf, const char *file);

extern void get_superuser_connection(UXconn **conn, UXconn **superuser_conn, UXconn **privileged_conn);

extern void make_remote_repmgr_path(UXSQLExpBufferData *outputbuf, t_node_info *remote_node_record);
extern void make_repmgrd_path(UXSQLExpBufferData *output_buf);

/* display functions */
extern bool format_node_status(t_node_info *node_info, UXSQLExpBufferData *node_status, UXSQLExpBufferData *upstream, ItemList *warnings);
extern void print_help_header(void);
extern void print_status_header(int cols, ColHeader *headers);

/* server control functions */
extern void get_server_action(t_server_action action, char *script, char *data_dir);
extern bool data_dir_required_for_action(t_server_action action);
extern void get_node_config_directory(char *config_dir_buf);
extern void get_node_data_directory(char *data_dir_buf);
extern void init_node_record(t_node_info *node_record);
extern bool can_use_ux_rewind(UXconn *conn, const char *data_directory, UXSQLExpBufferData *reason);
extern void make_standby_signal_path(const char *data_dir, char *buf);
extern bool write_standby_signal(const char *data_dir);

extern bool create_replication_slot(UXconn *conn, char *slot_name, t_node_info *upstream_node_record, UXSQLExpBufferData *error_msg);
extern bool drop_replication_slot_if_exists(UXconn *conn, int node_id, char *slot_name);

extern standy_join_status check_standby_join(UXconn *primary_conn, t_node_info *primary_node_record, t_node_info *standby_node_record);
extern bool check_replication_slots_available(int node_id, UXconn* conn);
extern bool check_node_can_attach(TimeLineID local_tli, XLogRecPtr local_xlogpos, UXconn *follow_target_conn, t_node_info *follow_target_node_record, bool is_rejoin);
extern bool check_replication_config_owner(int pg_version, const char *data_directory, UXSQLExpBufferData *error_msg, UXSQLExpBufferData *detail_msg);

extern void check_shared_library(UXconn *conn);
extern bool is_repmgrd_running(UXconn *conn);
extern int parse_repmgr_version(const char *version_string);

#endif							/* _REPMGR_CLIENT_GLOBAL_H_ */
