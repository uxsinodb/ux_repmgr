/*
 * dbutils.c - Database connection/management functions
 *
 * Portions Copyright (c) 2016-2022, Beijing Uxsino Software Limited, Co.
 * Copyright (c) 2009-2020, UXDB Software Co.,Ltd.
 *
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

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <arpa/inet.h>

#include "repmgr.h"
#include "repmgrd.h"
#include "dbutils.h"
#include "controldata.h"
#include "dirutil.h"

#define NODE_RECORD_PARAM_COUNT 13

static void log_db_error(UXconn *conn, const char *query_text, const char *fmt,...)
__attribute__((format(UX_PRINTF_ATTRIBUTE, 3, 4)));

static bool _is_server_available(const char *conninfo, bool quiet);

static UXconn *_establish_db_connection(const char *conninfo,
						 const bool exit_on_error,
						 const bool log_notice,
						 const bool verbose_only);

static UXconn * _establish_replication_connection_from_params(UXconn *conn, const char *conninfo, const char *repluser);

static UXconn *_get_primary_connection(UXconn *standby_conn, int *primary_id, char *primary_conninfo_out, bool quiet);

static bool _set_config(UXconn *conn, const char *config_param, const char *sqlquery);
static bool _get_ux_setting(UXconn *conn, const char *setting, char *str_output, bool *bool_output, int *int_output);

static RecordStatus _get_node_record(UXconn *conn, char *sqlquery, t_node_info *node_info, bool init_defaults);
static void _populate_node_record(UXresult *res, t_node_info *node_info, int row, bool init_defaults);

static void _populate_node_records(UXresult *res, NodeInfoList *node_list);

static bool _create_update_node_record(UXconn *conn, char *action, t_node_info *node_info);

static ReplSlotStatus _verify_replication_slot(UXconn *conn, char *slot_name, UXSQLExpBufferData *error_msg);

static bool _create_event(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info, bool send_notification);

static NodeAttached _is_downstream_node_attached(UXconn *conn, char *node_name, char **node_state, bool quiet);

static bool is_exist_bind_virtual_ip(const char *vip, const char *network_card);
static void arping_virtual_ip(void);

/*
 * This provides a standardized way of logging database errors. Note
 * that the provided UXconn can be a normal or a replication connection;
 * no attempt is made to write to the database, only to report the output
 * of UXSQLerrorMessage().
 */
void
log_db_error(UXconn *conn, const char *query_text, const char *fmt,...)
{
	va_list		ap;
	char		buf[MAXLEN];
	int			retval;

	va_start(ap, fmt);
	retval = vsnprintf(buf, MAXLEN, fmt, ap);
	va_end(ap);

	if (retval < MAXLEN)
		log_error("%s", buf);

	if (conn != NULL)
	{
		log_detail("\n%s", UXSQLerrorMessage(conn));
	}

	if (query_text != NULL)
	{
		log_detail("query text is:\n%s", query_text);
	}
}

/* ================= */
/* utility functions */
/* ================= */

XLogRecPtr
parse_lsn(const char *str)
{
	XLogRecPtr	ptr = InvalidXLogRecPtr;
	uint32		high,
				low;

	if (sscanf(str, "%x/%x", &high, &low) == 2)
		ptr = (((XLogRecPtr) high) << 32) + (XLogRecPtr) low;

	return ptr;
}


/* ==================== */
/* Connection functions */
/* ==================== */

/*
 * _establish_db_connection()
 *
 * Connect to a database using a conninfo string.
 *
 * NOTE: *do not* use this for replication connections; instead use:
 *	 establish_db_connection_by_params()
 */

static UXconn *
_establish_db_connection(const char *conninfo, const bool exit_on_error, const bool log_notice, const bool verbose_only)
{
	UXconn	   *conn = NULL;
	char	   *connection_string = NULL;
	char	   *errmsg = NULL;

	t_conninfo_param_list conninfo_params = T_CONNINFO_PARAM_LIST_INITIALIZER;
	bool		is_replication_connection = false;
	bool		parse_success = false;

	initialize_conninfo_params(&conninfo_params, false);

	parse_success = parse_conninfo_string(conninfo, &conninfo_params, &errmsg, false);

	if (parse_success == false)
	{
		log_error(_("unable to parse provided conninfo string \"%s\""), conninfo);
		log_detail("%s", errmsg);
		free_conninfo_params(&conninfo_params);
		return NULL;
	}

	/* set some default values if not explicitly provided */
	param_set_ine(&conninfo_params, "connect_timeout", "2");
	param_set_ine(&conninfo_params, "fallback_application_name", "repmgr");

	if (param_get(&conninfo_params, "replication") != NULL)
		is_replication_connection = true;

	/* use a secure search_path */
	param_set(&conninfo_params, "options", "-csearch_path=");

	connection_string = param_list_to_string(&conninfo_params);

	log_debug(_("connecting to: \"%s\""), connection_string);

	conn = UXSQLconnectdb(connection_string);

	/* Check to see that the backend connection was successfully made */
	if ((UXSQLstatus(conn) != CONNECTION_OK))
	{
		bool		emit_log = true;

		if (verbose_only == true && verbose_logging == false)
			emit_log = false;

		if (emit_log)
		{
			if (log_notice)
			{
				log_notice(_("connection to database failed"));
				log_detail("\n%s", UXSQLerrorMessage(conn));
			}
			else
			{
				log_error(_("connection to database failed"));
				log_detail("\n%s", UXSQLerrorMessage(conn));
			}
			log_detail(_("attempted to connect using:\n  %s"),
					   connection_string);
		}

		if (exit_on_error)
		{
			UXSQLfinish(conn);
			free_conninfo_params(&conninfo_params);
			exit(ERR_DB_CONN);
		}
	}

	/*
	 * set "synchronous_commit" to "local" in case synchronous replication is
	 * in use
	 *
	 * XXX set this explicitly before any write operations
	 */

	else if (is_replication_connection == false &&
			 set_config(conn, "synchronous_commit", "local") == false)
	{
		if (exit_on_error)
		{
			UXSQLfinish(conn);
			free_conninfo_params(&conninfo_params);
			exit(ERR_DB_CONN);
		}
	}

	pfree(connection_string);
	free_conninfo_params(&conninfo_params);

	return conn;
}


/*
 * Establish a database connection, optionally exit on error
 */
UXconn *
establish_db_connection(const char *conninfo, const bool exit_on_error)
{
	return _establish_db_connection(conninfo, exit_on_error, false, false);
}

/*
 * Attempt to establish a database connection, never exit on error, only
 * output error messages if --verbose option used
 */
UXconn *
establish_db_connection_quiet(const char *conninfo)
{
	return _establish_db_connection(conninfo, false, false, true);
}


UXconn *
establish_db_connection_with_replacement_param(const char *conninfo,
											   const char *param,
											   const char *value,
											   const bool exit_on_error)
{
	t_conninfo_param_list node_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
	char	   *errmsg = NULL;
	bool		parse_success = false;
	UXconn	   *conn = NULL;

	initialize_conninfo_params(&node_conninfo, false);

	parse_success = parse_conninfo_string(conninfo,
										  &node_conninfo,
										  &errmsg, false);

	if (parse_success == false)
	{
		log_error(_("unable to parse conninfo string \"%s\" for local node"),
				  conninfo);
		log_detail("%s", errmsg);

		if (exit_on_error == true)
			exit(ERR_BAD_CONFIG);

		return NULL;
	}

	param_set(&node_conninfo,
			  param,
			  value);

	conn = establish_db_connection_by_params(&node_conninfo, exit_on_error);

	free_conninfo_params(&node_conninfo);

	return conn;
}

UXconn *
establish_primary_db_connection(UXconn *conn,
								const bool exit_on_error)
{
	t_node_info primary_node_info = T_NODE_INFO_INITIALIZER;
	bool		primary_record_found = get_primary_node_record(conn, &primary_node_info);

	if (primary_record_found == false)
	{
		return NULL;
	}

	return establish_db_connection(primary_node_info.conninfo,
								   exit_on_error);
}


UXconn *
establish_db_connection_by_params(t_conninfo_param_list *param_list,
								  const bool exit_on_error)
{
	UXconn	   *conn = NULL;

	/* set some default values if not explicitly provided */
	param_set_ine(param_list, "connect_timeout", "2");
	param_set_ine(param_list, "fallback_application_name", "repmgr");

	/* use a secure search_path */
	param_set(param_list, "options", "-csearch_path=");

	/* Connect to the database using the provided parameters */
	conn = UXSQLconnectdbParams((const char **) param_list->keywords, (const char **) param_list->values, true);

	/* Check to see that the backend connection was successfully made */
	if ((UXSQLstatus(conn) != CONNECTION_OK))
	{
		log_error(_("connection to database failed"));
		log_detail("\n%s", UXSQLerrorMessage(conn));

		if (exit_on_error)
		{
			UXSQLfinish(conn);
			exit(ERR_DB_CONN);
		}
	}
	else
	{
		bool		is_replication_connection = false;
		int			i;

		/*
		 * set "synchronous_commit" to "local" in case synchronous replication
		 * is in use (provided this is not a replication connection)
		 */

		for (i = 0; param_list->keywords[i]; i++)
		{
			if (strcmp(param_list->keywords[i], "replication") == 0)
				is_replication_connection = true;
		}

		if (is_replication_connection == false && set_config(conn, "synchronous_commit", "local") == false)
		{
			if (exit_on_error)
			{
				UXSQLfinish(conn);
				exit(ERR_DB_CONN);
			}
		}
	}

	return conn;
}


/*
 * Given an existing active connection and the name of a replication
 * user, extract the connection parameters from that connection and
 * attempt to return a replication connection.
 */
UXconn *
establish_replication_connection_from_conn(UXconn *conn, const char *repluser)
{
	return _establish_replication_connection_from_params(conn, NULL, repluser);
}

UXconn *
establish_replication_connection_from_conninfo(const char *conninfo, const char *repluser)
{
	return _establish_replication_connection_from_params(NULL, conninfo, repluser);
}


static UXconn *
_establish_replication_connection_from_params(UXconn *conn, const char *conninfo, const char *repluser)
{
	t_conninfo_param_list repl_conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
	UXconn *repl_conn = NULL;

	initialize_conninfo_params(&repl_conninfo, false);

	if (conn != NULL)
		conn_to_param_list(conn, &repl_conninfo);
	else if (conninfo != NULL)
		parse_conninfo_string(conninfo, &repl_conninfo, NULL, false);

	/* Set the provided replication user */
	param_set(&repl_conninfo, "user", repluser);
	param_set(&repl_conninfo, "replication", "1");
	param_set(&repl_conninfo, "dbname", "replication");

	repl_conn = establish_db_connection_by_params(&repl_conninfo, false);
	free_conninfo_params(&repl_conninfo);

	return repl_conn;
}


UXconn *
get_primary_connection(UXconn *conn,
					   int *primary_id, char *primary_conninfo_out)
{
	return _get_primary_connection(conn, primary_id, primary_conninfo_out, false);
}


UXconn *
get_primary_connection_quiet(UXconn *conn,
							 int *primary_id, char *primary_conninfo_out)
{
	return _get_primary_connection(conn, primary_id, primary_conninfo_out, true);
}

UXconn *
duplicate_connection(UXconn *conn, const char *user, bool replication)
{
	t_conninfo_param_list conninfo = T_CONNINFO_PARAM_LIST_INITIALIZER;
	UXconn *duplicate_conn = NULL;

	initialize_conninfo_params(&conninfo, false);
	conn_to_param_list(conn, &conninfo);

	if (user != NULL)
		param_set(&conninfo, "user", user);

	if (replication == true)
		param_set(&conninfo, "replication", "1");

	duplicate_conn = establish_db_connection_by_params(&conninfo, false);

	free_conninfo_params(&conninfo);

	return duplicate_conn;
}



void
close_connection(UXconn **conn)
{
	if (*conn == NULL)
		return;

	UXSQLfinish(*conn);

	*conn = NULL;
}


/* =============================== */
/* conninfo manipulation functions */
/* =============================== */


/*
 * get_conninfo_value()
 *
 * Extract the value represented by 'keyword' in 'conninfo' and copy
 * it to the 'output' buffer.
 *
 * Returns true on success, or false on failure (conninfo string could
 * not be parsed, or provided keyword not found).
 */

bool
get_conninfo_value(const char *conninfo, const char *keyword, char *output)
{
	UXSQLconninfoOption *conninfo_options = NULL;
	UXSQLconninfoOption *conninfo_option = NULL;

	conninfo_options = UXSQLconninfoParse(conninfo, NULL);

	if (conninfo_options == NULL)
	{
		log_error(_("unable to parse provided conninfo string \"%s\""), conninfo);
		return false;
	}

	for (conninfo_option = conninfo_options; conninfo_option->keyword != NULL; conninfo_option++)
	{
		if (strcmp(conninfo_option->keyword, keyword) == 0)
		{
			if (conninfo_option->val != NULL && conninfo_option->val[0] != '\0')
			{
				strncpy(output, conninfo_option->val, MAXLEN);
				break;
			}
		}
	}

	UXSQLconninfoFree(conninfo_options);

	return true;
}


/*
 * Get a default conninfo value for the provided parameter, and copy
 * it to the 'output' buffer.
 *
 * Returns true on success, or false on failure (provided keyword not found).
 *
 */
bool
get_conninfo_default_value(const char *param, char *output, int maxlen)
{
	UXSQLconninfoOption *defs = NULL;
	UXSQLconninfoOption *def = NULL;
	bool found = false;

	defs = UXSQLconndefaults();

	for (def = defs; def->keyword; def++)
	{
		if (strncmp(def->keyword, param, maxlen) == 0)
		{
			strncpy(output, def->val, maxlen);
			found = true;
		}
	}

	UXSQLconninfoFree(defs);

	return found;
}


void
initialize_conninfo_params(t_conninfo_param_list *param_list, bool set_defaults)
{
	UXSQLconninfoOption *defs = NULL;
	UXSQLconninfoOption *def = NULL;
	int			c;

	defs = UXSQLconndefaults();
	param_list->size = 0;

	/* Count maximum number of parameters */
	for (def = defs; def->keyword; def++)
		param_list->size++;

	/* Initialize our internal parameter list */
	param_list->keywords = ux_malloc0(sizeof(char *) * (param_list->size + 1));
	param_list->values = ux_malloc0(sizeof(char *) * (param_list->size + 1));

	for (c = 0; c < param_list->size; c++)
	{
		param_list->keywords[c] = NULL;
		param_list->values[c] = NULL;
	}

	if (set_defaults == true)
	{
		/* Pre-set any defaults */

		for (def = defs; def->keyword; def++)
		{
			if (def->val != NULL && def->val[0] != '\0')
			{
				param_set(param_list, def->keyword, def->val);
			}
		}
	}

	UXSQLconninfoFree(defs);
}


void
free_conninfo_params(t_conninfo_param_list *param_list)
{
	int			c;

	for (c = 0; c < param_list->size; c++)
	{
		if (param_list->keywords != NULL && param_list->keywords[c] != NULL)
			pfree(param_list->keywords[c]);

		if (param_list->values != NULL && param_list->values[c] != NULL)
			pfree(param_list->values[c]);
	}
	/* BEGIN Added by chen_jingwen for #207866 at 2024/10/8 */
	/* 释放空间后置空，避免后续非法内存访问 */
	param_list->size = 0;

	if (param_list->keywords != NULL)
	{
		pfree(param_list->keywords);
		param_list->keywords = NULL;
	}

	if (param_list->values != NULL)
	{
		pfree(param_list->values);
		param_list->values = NULL;
	}
	/* END Added by chen_jingwen for #207866 at 2024/10/8 */
}



void
copy_conninfo_params(t_conninfo_param_list *dest_list, t_conninfo_param_list *source_list)
{
	int			c;

	for (c = 0; c < source_list->size && source_list->keywords[c] != NULL; c++)
	{
		if (source_list->values[c] != NULL && source_list->values[c][0] != '\0')
		{
			param_set(dest_list, source_list->keywords[c], source_list->values[c]);
		}
	}
}

void
param_set(t_conninfo_param_list *param_list, const char *param, const char *value)
{
	int			c;
	int			value_len = strlen(value) + 1;
	int			param_len;

	/*
	 * Scan array to see if the parameter is already set - if not, replace it
	 */
	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			if (param_list->values[c] != NULL)
				pfree(param_list->values[c]);

			param_list->values[c] = ux_malloc0(value_len);
			strncpy(param_list->values[c], value, value_len);

			return;
		}
	}

	/*
	 * Sanity-check that the caller is not trying to overflow the array;
	 * in practice this is highly unlikely, and if it ever happens, this means
	 * something is highly wrong.
	 */
	Assert(c < param_list->size);

	/*
	 * Parameter not in array - add it and its associated value
	 */
	param_len = strlen(param) + 1;

	param_list->keywords[c] = ux_malloc0(param_len);
	param_list->values[c] = ux_malloc0(value_len);

	strncpy(param_list->keywords[c], param, param_len);
	strncpy(param_list->values[c], value, value_len);
}


/*
 * Like param_set(), but will only set the parameter if it doesn't exist
 */
void
param_set_ine(t_conninfo_param_list *param_list, const char *param, const char *value)
{
	int			c;
	int			value_len = strlen(value) + 1;
	int			param_len;

	/*
	 * Scan array to see if the parameter is already set - if so, do nothing
	 */
	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			/* parameter exists, do nothing */
			return;
		}
	}

	/*
	 * Sanity-check that the caller is not trying to overflow the array;
	 * in practice this is highly unlikely, and if it ever happens, this means
	 * something is highly wrong.
	 */
	Assert(c < param_list->size);

	/*
	 * Parameter not in array - add it and its associated value
	 */
	param_len = strlen(param) + 1;

	param_list->keywords[c] = ux_malloc0(param_len);
	param_list->values[c] = ux_malloc0(value_len);

	strncpy(param_list->keywords[c], param, param_len);
	strncpy(param_list->values[c], value, value_len);
}


char *
param_get(t_conninfo_param_list *param_list, const char *param)
{
	int			c;

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (strcmp(param_list->keywords[c], param) == 0)
		{
			if (param_list->values[c] != NULL && param_list->values[c][0] != '\0')
				return param_list->values[c];
			else
				return NULL;
		}
	}

	return NULL;
}


/*
 * Validate a conninfo string by attempting to parse it.
 *
 * "errmsg": passed to UXSQLconninfoParse(), may be NULL
 *
 * NOTE: UXSQLconninfoParse() verifies the string format and checks for
 * valid options but does not sanity check values.
 */

bool
validate_conninfo_string(const char *conninfo_str, char **errmsg)
{
	UXSQLconninfoOption *connOptions = NULL;

	connOptions = UXSQLconninfoParse(conninfo_str, errmsg);

	if (connOptions == NULL)
		return false;

	UXSQLconninfoFree(connOptions);

	return true;
}


/*
 * Parse a conninfo string into a t_conninfo_param_list
 *
 * See conn_to_param_list() to do the same for a UXconn
 *
 * "errmsg": passed to UXSQLconninfoParse(), may be NULL
 *
 * "ignore_local_params": ignores those parameters specific
 * to a local installation, i.e. when parsing an upstream
 * node's conninfo string for inclusion into "primary_conninfo",
 * don't copy that node's values
 */
bool
parse_conninfo_string(const char *conninfo_str, t_conninfo_param_list *param_list, char **errmsg, bool ignore_local_params)
{
	UXSQLconninfoOption *connOptions = NULL;
	UXSQLconninfoOption *option = NULL;

	connOptions = UXSQLconninfoParse(conninfo_str, errmsg);

	if (connOptions == NULL)
		return false;

	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values */
		if ((option->val == NULL) || option->val[0] == '\0')
			continue;

		/* Ignore settings specific to the upstream node */
		if (ignore_local_params == true)
		{
			if (strcmp(option->keyword, "application_name") == 0)
				continue;
			if (strcmp(option->keyword, "passfile") == 0)
				continue;
			if (strcmp(option->keyword, "servicefile") == 0)
				continue;
		}
		param_set(param_list, option->keyword, option->val);
	}

	UXSQLconninfoFree(connOptions);

	return true;
}


/*
 * Parse a UXconn into a t_conninfo_param_list
 *
 * See parse_conninfo_string() to do the same for a conninfo string
 *
 * NOTE: the current use case for this is to take an active connection,
 * replace the existing username (typically replacing it with the superuser
 * or replication user name), and make a new connection as that user.
 * If the "password" field is set, it will cause any connection made with
 * these parameters to fail (unless of course the password happens to be the
 * same). Therefore we remove the password altogether, and rely on it being
 * available via .uxpass.
 */
void
conn_to_param_list(UXconn *conn, t_conninfo_param_list *param_list)
{
	UXSQLconninfoOption *connOptions = NULL;
	UXSQLconninfoOption *option = NULL;

	connOptions = UXSQLconninfo(conn);
	for (option = connOptions; option && option->keyword; option++)
	{
		/* Ignore non-set or blank parameter values */
		if ((option->val == NULL) || option->val[0] == '\0')
			continue;

		/* Ignore "password" */
		if (strcmp(option->keyword, "password") == 0)
			continue;

		param_set(param_list, option->keyword, option->val);
	}

	UXSQLconninfoFree(connOptions);
}


/*
 * Converts param list to string; caller must free returned pointer
 */
char *
param_list_to_string(t_conninfo_param_list *param_list)
{
	int			c;
	UXSQLExpBufferData conninfo_buf;
	char	   *conninfo_str = NULL;
	int			len = 0;

	initUXSQLExpBuffer(&conninfo_buf);

	for (c = 0; c < param_list->size && param_list->keywords[c] != NULL; c++)
	{
		if (param_list->values[c] != NULL && param_list->values[c][0] != '\0')
		{
			if (c > 0)
				appendUXSQLExpBufferChar(&conninfo_buf, ' ');

			/* XXX escape value */
			appendUXSQLExpBuffer(&conninfo_buf,
							  "%s=%s",
							  param_list->keywords[c],
							  param_list->values[c]);
		}
	}

	len = strlen(conninfo_buf.data) + 1;
	conninfo_str = ux_malloc0(len);

	strncpy(conninfo_str, conninfo_buf.data, len);

	termUXSQLExpBuffer(&conninfo_buf);

	return conninfo_str;
}


/*
 * Run a conninfo string through the parser, and pass it back as a normal
 * conninfo string. This is mainly intended for converting connection URIs
 * to parameter/value conninfo strings.
 *
 * Caller must free returned pointer.
 */

char *
normalize_conninfo_string(const char *conninfo_str)
{
	t_conninfo_param_list conninfo_params = T_CONNINFO_PARAM_LIST_INITIALIZER;
	bool		parse_success = false;
	char	   *normalized_string = NULL;
	char	   *errmsg = NULL;

	initialize_conninfo_params(&conninfo_params, false);

	parse_success = parse_conninfo_string(conninfo_str, &conninfo_params, &errmsg, false);

	if (parse_success == false)
	{
		log_error(_("unable to parse provided conninfo string \"%s\""), conninfo_str);
		log_detail("%s", errmsg);
		free_conninfo_params(&conninfo_params);
		return NULL;
	}


	normalized_string = param_list_to_string(&conninfo_params);
	free_conninfo_params(&conninfo_params);

	return normalized_string;
}

/*
 * check whether the libuxsql version in use recognizes the "passfile" parameter
 * (should be 9.6 and later)
 */
bool
has_passfile(void)
{
	UXSQLconninfoOption *defs = UXSQLconndefaults();
	UXSQLconninfoOption *def = NULL;
	bool has_passfile = false;

   	for (def = defs; def->keyword; def++)
	{
		if (strcmp(def->keyword, "passfile") == 0)
		{
			has_passfile = true;
			break;
		}
	}

	UXSQLconninfoFree(defs);

	return has_passfile;
}



/* ===================== */
/* transaction functions */
/* ===================== */

bool
begin_transaction(UXconn *conn)
{
	UXresult   *res = NULL;

	log_verbose(LOG_DEBUG, "begin_transaction()");

	res = UXSQLexec(conn, "BEGIN");

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_error(_("unable to begin transaction"));
		log_detail("%s", UXSQLerrorMessage(conn));

		UXSQLclear(res);
		return false;
	}

	UXSQLclear(res);

	return true;
}


bool
commit_transaction(UXconn *conn)
{
	UXresult   *res = NULL;

	log_verbose(LOG_DEBUG, "commit_transaction()");

	res = UXSQLexec(conn, "COMMIT");

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_error(_("unable to commit transaction"));
		log_detail("%s", UXSQLerrorMessage(conn));
		UXSQLclear(res);

		return false;
	}

	UXSQLclear(res);

	return true;
}


bool
rollback_transaction(UXconn *conn)
{
	UXresult   *res = NULL;

	log_verbose(LOG_DEBUG, "rollback_transaction()");

	res = UXSQLexec(conn, "ROLLBACK");

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_error(_("unable to rollback transaction"));
		log_detail("%s", UXSQLerrorMessage(conn));
		UXSQLclear(res);

		return false;
	}

	UXSQLclear(res);

	return true;
}


/* ========================== */
/* GUC manipulation functions */
/* ========================== */

static bool
_set_config(UXconn *conn, const char *config_param, const char *sqlquery)
{
	bool		success = true;
	UXresult   *res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, sqlquery, "_set_config(): unable to set \"%s\"", config_param);
		success = false;
	}

	UXSQLclear(res);

	return success;
}


bool
set_config(UXconn *conn, const char *config_param, const char *config_value)
{
	UXSQLExpBufferData query;
	bool		result = false;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "SET %s TO '%s'",
					  config_param,
					  config_value);

	log_verbose(LOG_DEBUG, "set_config():\n  %s", query.data);

	result = _set_config(conn, config_param, query.data);

	termUXSQLExpBuffer(&query);

	return result;
}

bool
set_config_bool(UXconn *conn, const char *config_param, bool state)
{
	UXSQLExpBufferData query;
	bool		result = false;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "SET %s TO %s",
					  config_param,
					  state ? "TRUE" : "FALSE");

	log_verbose(LOG_DEBUG, "set_config_bool():\n  %s", query.data);


	result = _set_config(conn, config_param, query.data);

	termUXSQLExpBuffer(&query);

	return result;
}


int
guc_set(UXconn *conn, const char *parameter, const char *op,
		const char *value)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			retval = 1;

	char	   *escaped_parameter = escape_string(conn, parameter);
	char	   *escaped_value = escape_string(conn, value);

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "SELECT true FROM ux_catalog.ux_settings "
					  " WHERE name = '%s' AND setting %s '%s'",
					  escaped_parameter, op, escaped_value);

	log_verbose(LOG_DEBUG, "guc_set():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("guc_set(): unable to execute query"));
		retval = -1;
	}
	else if (UXSQLntuples(res) == 0)
	{
		retval = 0;
	}

	pfree(escaped_parameter);
	pfree(escaped_value);
	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return retval;
}


bool
get_ux_setting(UXconn *conn, const char *setting, char *output)
{
	bool success = _get_ux_setting(conn, setting, output, NULL, NULL);

	if (success == true)
	{
		log_verbose(LOG_DEBUG, _("get_ux_setting(): returned value is \"%s\""), output);
	}

	return success;
}

bool
get_ux_setting_bool(UXconn *conn, const char *setting, bool *output)
{
	bool success = _get_ux_setting(conn, setting, NULL, output, NULL);

	if (success == true)
	{
		log_verbose(LOG_DEBUG, _("get_ux_setting(): returned value is \"%s\""),
					*output == true ? "TRUE" : "FALSE");
	}

	return success;
}

bool
get_ux_setting_int(UXconn *conn, const char *setting, int *output)
{
	bool success = _get_ux_setting(conn, setting, NULL, NULL, output);

	if (success == true)
	{
		log_verbose(LOG_DEBUG, _("get_ux_setting_int(): returned value is \"%i\""), *output);
	}

	return success;
}


bool
_get_ux_setting(UXconn *conn, const char *setting, char *str_output, bool *bool_output, int *int_output)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			i;
	bool		success = false;

	char	   *escaped_setting = escape_string(conn, setting);

	if (escaped_setting == NULL)
	{
		log_error(_("unable to escape setting \"%s\""), setting);
		return false;
	}

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "SELECT name, setting "
					  "  FROM ux_catalog.ux_settings WHERE name = '%s'",
					  escaped_setting);

	log_verbose(LOG_DEBUG, "get_ux_setting():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	pfree(escaped_setting);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_ux_setting() - unable to execute query"));

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		return false;
	}

	for (i = 0; i < UXSQLntuples(res); i++)
	{
		if (strcmp(UXSQLgetvalue(res, i, 0), setting) == 0)
		{
			if (str_output != NULL)
			{
				snprintf(str_output, MAXLEN, "%s", UXSQLgetvalue(res, i, 1));
			}
			else if (bool_output != NULL)
			{
				/*
				 * Note we assume the caller is sure this is a boolean parameter
				 */
				if (strncmp(UXSQLgetvalue(res, i, 1), "on", MAXLEN) == 0)
					*bool_output = true;
				else
					*bool_output = false;
			}
			else if (int_output != NULL)
			{
				*int_output = atoi(UXSQLgetvalue(res, i, 1));
			}

			success = true;
			break;
		}
		else
		{
			/* highly unlikely this would ever happen */
			log_error(_("get_ux_setting(): unknown parameter \"%s\""), UXSQLgetvalue(res, i, 0));
		}
	}


	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}



bool
alter_system_int(UXconn *conn, const char *name, int value)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "ALTER SYSTEM SET %s = %i",
					  name, value);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("alter_system_int() - unable to execute query"));

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


bool
ux_reload_conf(UXconn *conn)
{
	UXresult   *res = NULL;
	bool		success = true;

	res = UXSQLexec(conn, "SELECT ux_catalog.ux_reload_conf()");

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, NULL, _("ux_reload_conf() - unable to execute query"));

		success = false;
	}

	UXSQLclear(res);

	return success;
}


/*
 * add by houjiaxing for #201574 at 2024/1/29 reviewer:wangbocai
 * 调用 ALTER SYSTEM 修改 uxdb 参数
 */
bool
alter_system_str(UXconn *conn, const char *name, const char *value)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "ALTER SYSTEM SET %s = %s",
					  name, value);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("alter_system_str() - unable to execute query"));

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


/* ============================ */
/* Server information functions */
/* ============================ */


bool
get_cluster_size(UXconn *conn, char *size)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBufferStr(&query,
						 "SELECT ux_catalog.ux_size_pretty(ux_catalog.sum(ux_catalog.ux_database_size(oid))::bigint) "
						 " FROM ux_catalog.ux_database ");

	log_verbose(LOG_DEBUG, "get_cluster_size():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
		success = false;
	else
		snprintf(size, MAXLEN, "%s", UXSQLgetvalue(res, 0, 0));

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


/*
 * Return the server version number for the connection provided
 */
int
get_server_version(UXconn *conn, char *server_version_buf)
{
	UXresult   *res = NULL;
	int			_server_version_num = UNKNOWN_SERVER_VERSION_NUM;

	const char	   *sqlquery =
		"SELECT ux_catalog.current_setting('server_version_num'), "
		"       ux_catalog.current_setting('server_version')";

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to determine server version number"));
		UXSQLclear(res);

		return UNKNOWN_SERVER_VERSION_NUM;
	}

	_server_version_num = atoi(UXSQLgetvalue(res, 0, 0));

	if (server_version_buf != NULL)
	{
		int			i;
		char		_server_version_buf[MAXVERSIONSTR] = "";

		memset(_server_version_buf, 0, MAXVERSIONSTR);

		/*
		 * Some distributions may add extra info after the actual version number,
		 * e.g. "10.4 (Debian 10.4-2.uxdg90+1)", so copy everything up until the
		 * first space.
		 */

		snprintf(_server_version_buf, MAXVERSIONSTR, "%s", UXSQLgetvalue(res, 0, 1));

		for (i = 0; i < MAXVERSIONSTR; i++)
		{
			if (_server_version_buf[i] == ' ')
				break;

			*server_version_buf++ = _server_version_buf[i];
		}
	}

	UXSQLclear(res);

	return _server_version_num;
}


RecoveryType
get_recovery_type(UXconn *conn)
{
	UXresult   *res = NULL;
	RecoveryType recovery_type = RECTYPE_UNKNOWN;

	const char	   *sqlquery = "SELECT ux_catalog.ux_is_in_recovery()";

	log_verbose(LOG_DEBUG, "get_recovery_type(): %s", sqlquery);

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn,
					 sqlquery,
					 _("unable to determine if server is in recovery"));

		recovery_type = RECTYPE_UNKNOWN;
	}
	else if (UXSQLntuples(res) == 1)
	{
		if (strcmp(UXSQLgetvalue(res, 0, 0), "f") == 0)
		{
			recovery_type = RECTYPE_PRIMARY;
		}
		else
		{
			recovery_type = RECTYPE_STANDBY;
		}
	}

	UXSQLclear(res);

	return recovery_type;
}

/*
 * Read the node list from the provided connection and attempt to connect to each node
 * in turn to definitely establish if it's the cluster primary.
 *
 * The node list is returned in the order which makes it likely that the
 * current primary will be returned first, reducing the number of speculative
 * connections which need to be made to other nodes.
 *
 * If primary_conninfo_out points to allocated memory of MAXCONNINFO in length,
 * the primary server's conninfo string will be copied there.
 */

UXconn *
_get_primary_connection(UXconn *conn,
						int *primary_id, char *primary_conninfo_out, bool quiet)
{
	UXSQLExpBufferData query;

	UXconn	   *remote_conn = NULL;
	UXresult   *res = NULL;

	char		remote_conninfo_stack[MAXCONNINFO];
	char	   *remote_conninfo = &*remote_conninfo_stack;

	int			i,
				node_id;

	/*
	 * If the caller wanted to get a copy of the connection info string, sub
	 * out the local stack pointer for the pointer passed by the caller.
	 */
	if (primary_conninfo_out != NULL)
		remote_conninfo = primary_conninfo_out;

	if (primary_id != NULL)
	{
		*primary_id = NODE_NOT_FOUND;
	}

	/* find all registered nodes  */
	log_verbose(LOG_INFO, _("searching for primary node"));

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBufferStr(&query,
						 "  SELECT node_id, conninfo, "
						 "         CASE WHEN type = 'primary' THEN 1 ELSE 2 END AS type_priority"
						 "	   FROM repmgr.nodes "
						 "   WHERE active IS TRUE "
						 "     AND type != 'witness' "
						 "ORDER BY active DESC, type_priority, priority, node_id");

	log_verbose(LOG_DEBUG, "get_primary_connection():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("_get_primary_connection(): unable to retrieve node records"));

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		return NULL;
	}

	termUXSQLExpBuffer(&query);

	for (i = 0; i < UXSQLntuples(res); i++)
	{
		RecoveryType recovery_type;

		/* initialize with the values of the current node being processed */
		node_id = atoi(UXSQLgetvalue(res, i, 0));
		snprintf(remote_conninfo, MAXCONNINFO, "%s", UXSQLgetvalue(res, i, 1));

		log_verbose(LOG_INFO,
					_("checking if node %i is primary"),
					node_id);

		if (quiet)
		{
			remote_conn = establish_db_connection_quiet(remote_conninfo);
		}
		else
		{
			remote_conn = establish_db_connection(remote_conninfo, false);
		}

		if (UXSQLstatus(remote_conn) != CONNECTION_OK)
		{
			UXSQLfinish(remote_conn);
			remote_conn = NULL;
			continue;
		}

		recovery_type = get_recovery_type(remote_conn);

		if (recovery_type == RECTYPE_UNKNOWN)
		{
			log_warning(_("unable to retrieve recovery state from node %i"),
						node_id);

			UXSQLfinish(remote_conn);
			continue;
		}

		if (recovery_type == RECTYPE_PRIMARY)
		{
			UXSQLclear(res);
			log_verbose(LOG_INFO, _("current primary node is %i"), node_id);

			if (primary_id != NULL)
			{
				*primary_id = node_id;
			}

			if (extra_remote_conn)
			{
				UXSQLfinish(extra_remote_conn);
				extra_remote_conn = NULL;
			}

			return remote_conn;
		}

		UXSQLfinish(remote_conn);
	}

	UXSQLclear(res);
	return NULL;
}



/*
 * Return the id of the active primary node, or NODE_NOT_FOUND if no
 * record available.
 *
 * This reports the value stored in the database only and
 * does not verify whether the node is actually available
 */
int
get_primary_node_id(UXconn *conn)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			retval = NODE_NOT_FOUND;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBufferStr(&query,
						 "SELECT node_id		  "
						 "	 FROM repmgr.nodes    "
						 " WHERE type = 'primary' "
						 "   AND active IS TRUE  ");

	log_verbose(LOG_DEBUG, "get_primary_node_id():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_primary_node_id(): unable to execute query"));
		retval = UNKNOWN_NODE_ID;
	}
	else if (UXSQLntuples(res) == 0)
	{
		log_verbose(LOG_WARNING, _("get_primary_node_id(): no active primary found"));
		retval = NODE_NOT_FOUND;
	}
	else
	{
		retval = atoi(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return retval;
}




int
get_ready_archive_files(UXconn *conn, const char *data_directory)
{
	char		archive_status_dir[MAXUXPATH] = "";
	struct stat statbuf;
	struct dirent *arcdir_ent;
	DIR		   *arcdir;

	int			ready_count = 0;

	if (UXSQLserverVersion(conn) >= 100000)
	{
		snprintf(archive_status_dir, MAXUXPATH,
				 "%s/ux_wal/archive_status",
				 data_directory);
	}
	else
	{
		snprintf(archive_status_dir, MAXUXPATH,
				 "%s/ux_xlog/archive_status",
				 data_directory);
	}

	/* sanity-check directory path */
	if (stat(archive_status_dir, &statbuf) == -1)
	{
		log_error(_("unable to access archive_status directory \"%s\""),
				  archive_status_dir);
		log_detail("%s", strerror(errno));

		return ARCHIVE_STATUS_DIR_ERROR;
	}

	arcdir = opendir(archive_status_dir);

	if (arcdir == NULL)
	{
		log_error(_("unable to open archive directory \"%s\""),
				  archive_status_dir);
		log_detail("%s", strerror(errno));

		return ARCHIVE_STATUS_DIR_ERROR;
	}

	while ((arcdir_ent = readdir(arcdir)) != NULL)
	{
		struct stat statbuf;
		char		file_path[MAXUXPATH + sizeof(arcdir_ent->d_name)];
		int			basenamelen = 0;

		snprintf(file_path, sizeof(file_path),
				 "%s/%s",
				 archive_status_dir,
				 arcdir_ent->d_name);

		/* skip non-files */
		if (stat(file_path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode))
		{
			continue;
		}

		basenamelen = (int) strlen(arcdir_ent->d_name) - 6;

		/*
		 * count anything ending in ".ready"; for a more precise
		 * implementation see: src/backend/uxmaster/uxarch.c
		 */
		if (strcmp(arcdir_ent->d_name + basenamelen, ".ready") == 0)
			ready_count++;
	}

	closedir(arcdir);

	return ready_count;
}



bool
identify_system(UXconn *repl_conn, t_system_identification *identification)
{
	UXresult   *res = NULL;

	/* semicolon required here */
	res = UXSQLexec(repl_conn, "IDENTIFY_SYSTEM;");

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK || !UXSQLntuples(res))
	{
		log_db_error(repl_conn, NULL, _("unable to execute IDENTIFY_SYSTEM"));

		UXSQLclear(res);
		return false;
	}

#if defined(__i386__) || defined(__i386)
	identification->system_identifier = atoll(UXSQLgetvalue(res, 0, 0));
#else
	identification->system_identifier = atol(UXSQLgetvalue(res, 0, 0));
#endif

	identification->timeline = atoi(UXSQLgetvalue(res, 0, 1));
	identification->xlogpos = parse_lsn(UXSQLgetvalue(res, 0, 2));

	UXSQLclear(res);
	return true;
}

/*
 * Return the system identifier by querying ux_control_system().
 *
 * Note there is a similar function in controldata.c ("get_system_identifier()")
 * which reads the control file.
 */
uint64
system_identifier(UXconn *conn)
{
	uint64		system_identifier = UNKNOWN_SYSTEM_IDENTIFIER;
	UXresult   *res = NULL;

	/*
	 * ux_control_system() was introduced in UxsinoDB 9.6
	 */
	if (UXSQLserverVersion(conn) < 90600)
	{
		return UNKNOWN_SYSTEM_IDENTIFIER;
	}

	res = UXSQLexec(conn, "SELECT system_identifier FROM ux_catalog.ux_control_system()");

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, NULL, _("system_identifier(): unable to query ux_control_system()"));
	}
	else
	{
#if defined(__i386__) || defined(__i386)
		system_identifier = atoll(UXSQLgetvalue(res, 0, 0));
#else
		system_identifier = atol(UXSQLgetvalue(res, 0, 0));
#endif
	}

	UXSQLclear(res);

	return system_identifier;
}


TimeLineHistoryEntry *
get_timeline_history(UXconn *repl_conn, TimeLineID tli)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	UXSQLExpBufferData result;
	char		*resptr;

	TimeLineHistoryEntry *history;
	TimeLineID	file_tli = UNKNOWN_TIMELINE_ID;
	uint32		switchpoint_hi;
	uint32		switchpoint_lo;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "TIMELINE_HISTORY %i",
					  (int)tli);

	res = UXSQLexec(repl_conn, query.data);
	log_verbose(LOG_DEBUG, "get_timeline_history():\n%s", query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(repl_conn, query.data, _("get_timeline_history(): unable to execute query"));
		termUXSQLExpBuffer(&query);
		UXSQLclear(res);
		return NULL;
	}

	termUXSQLExpBuffer(&query);

	if (UXSQLntuples(res) != 1 || UXSQLnfields(res) != 2)
	{
		log_error(_("unexpected response to TIMELINE_HISTORY command"));
		log_detail(_("got %i rows and %i fields, expected %i rows and %i fields"),
				   UXSQLntuples(res), UXSQLnfields(res), 1, 2);
		UXSQLclear(res);
		return NULL;
	}

	initUXSQLExpBuffer(&result);
	appendUXSQLExpBufferStr(&result, UXSQLgetvalue(res, 0, 1));
	UXSQLclear(res);

	resptr = result.data;

	while (*resptr)
	{
		char	buf[MAXLEN];
		char   *bufptr = buf;

		if (*resptr != '\n')
		{
			int		len  = 0;

			memset(buf, 0, MAXLEN);

			while (*resptr && *resptr != '\n' && len < MAXLEN)
			{
				*bufptr++ = *resptr++;
				len++;
			}

			if (buf[0])
			{
				int nfields = sscanf(buf,
									 "%u\t%X/%X",
									 &file_tli, &switchpoint_hi, &switchpoint_lo);
				if (nfields == 3 && file_tli == tli - 1)
					break;
			}
		}

		if (*resptr)
			resptr++;
	}

	termUXSQLExpBuffer(&result);

	if (file_tli == UNKNOWN_TIMELINE_ID || file_tli != tli - 1)
	{
		log_error(_("timeline %i not found in timeline history file content"), tli);
		log_detail(_("content is: \"%s\""), result.data);
		return NULL;
	}

	history = (TimeLineHistoryEntry *) palloc(sizeof(TimeLineHistoryEntry));
	history->tli = file_tli;
	history->begin = InvalidXLogRecPtr; /* we don't care about this */
	history->end = ((uint64) (switchpoint_hi)) << 32 | (uint64) switchpoint_lo;

	return history;
}


pid_t
get_wal_receiver_pid(UXconn *conn)
{
	UXresult   *res = NULL;
	pid_t		wal_receiver_pid = UNKNOWN_PID;

	res = UXSQLexec(conn, "SELECT repmgr.get_wal_receiver_pid()");

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.get_wal_receiver_pid()\""));
		log_detail("%s", UXSQLerrorMessage(conn));
	}
	else if (!UXSQLgetisnull(res, 0, 0))
	{
		wal_receiver_pid = atoi(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);

	return wal_receiver_pid;
}


/* =============================== */
/* user/role information functions */
/* =============================== */


bool
can_execute_ux_promote(UXconn *conn)
{
	UXSQLExpBufferData query;
	UXresult   *res;
	bool		has_ux_promote= false;

	/* ux_promote() available from UxsinoDB 12 */
	if(UXSQLserverVersion(conn) < 120000)
		return false;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBufferStr(&query,
						 " SELECT ux_catalog.has_function_privilege( "
						 "    CURRENT_USER, "
						 "    'ux_catalog.ux_promote(bool,int)', "
						 "    'execute' "
						 " )");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("can_execute_ux_promote(): unable to query user function privilege"));
	}
	else
	{
		has_ux_promote = atobool(UXSQLgetvalue(res, 0, 0));
	}
	termUXSQLExpBuffer(&query);
	UXSQLclear(res);
	res = NULL;

	return has_ux_promote;
}


/*
 * Determine if the user associated with the current connection
 * has sufficient permissions to disable the walsender
 */
bool
can_disable_walsender(UXconn *conn)
{
	/*
	 * Requires UxsinoDB 9.5 or later, because ALTER SYSTEM
	 */
	if (UXSQLserverVersion(conn) < 90500)
	{
		log_warning(_("\"standby_disconnect_on_failover\" specified, but not available for this UxsinoDB version"));
		/* TODO: format server version */
		log_detail(_("available from UxsinoDB 9.5; this UxsinoDB version is %i"), UXSQLserverVersion(conn));

		return false;
	}

	/*
	 * Superusers can do anything
	 */
	if (is_superuser_connection(conn, NULL) == true)
		return true;

	/*
	 * As of UxsinoDB 14, it is not possible for a non-superuser
	 * to execute ALTER SYSTEM, so further checks are superfluous.
	 * This will need modifying for UxsinoDB 15.
	 */
	log_warning(_("\"standby_disconnect_on_failover\" specified, but repmgr user is not a superuser"));
	log_detail(_("superuser permission required to disable standbys on failover"));

	return false;
}

/*
 * Determine if the user associated with the current connection is
 * a member of the "ux_monitor" default role, or optionally one
 * of its three constituent "subroles".
 */
bool
connection_has_ux_monitor_role(UXconn *conn, const char *subrole)
{
	UXSQLExpBufferData query;
	UXresult   *res;
	bool		has_ux_monitor_role = false;

	/* superusers can read anything, no role check needed */
	if (is_superuser_connection(conn, NULL) == true)
		return true;

	/* ux_monitor and associated "subroles" introduced in UxsinoDB 10 */
	if (UXSQLserverVersion(conn) < 100000)
		return false;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBufferStr(&query,
						 "  SELECT CASE "
						 "           WHEN ux_catalog.ux_has_role('ux_monitor','MEMBER') "
						 "             THEN TRUE ");

	if (subrole != NULL)
	{
		appendUXSQLExpBuffer(&query,
						  "           WHEN ux_catalog.ux_has_role('%s','MEMBER') "
						  "             THEN TRUE ",
						  subrole);
	}

	appendUXSQLExpBufferStr(&query,
						 "           ELSE FALSE "
						 "         END AS has_ux_monitor");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("connection_has_ux_monitor_role(): unable to query user roles"));
	}
	else
	{
		has_ux_monitor_role = atobool(UXSQLgetvalue(res, 0, 0));
	}
	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return has_ux_monitor_role;
}

bool
is_replication_role(UXconn *conn, char *rolname)
{
	UXSQLExpBufferData query;
	UXresult   *res;
	bool		is_replication_role = false;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "  SELECT rolreplication "
						 "    FROM ux_catalog.ux_roles "
						 "   WHERE rolname = ");

	if (rolname != NULL)
	{
		appendUXSQLExpBuffer(&query,
						  "'%s'",
						  rolname);
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 "CURRENT_USER");
	}

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("is_replication_role(): unable to query user roles"));
	}
	else
	{
		is_replication_role = atobool(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return is_replication_role;
}


bool
is_superuser_connection(UXconn *conn, t_connection_user *userinfo)
{
	bool		is_superuser = false;
	const char *current_user = UXSQLuser(conn);
	const char *superuser_status = UXSQLparameterStatus(conn, "is_superuser");

	/* #169199 when disconnected from the primary node, superuser_status is empty */
	if (superuser_status)
		is_superuser = (strcmp(superuser_status, "on") == 0) ? true : false;

	/* Add by wangqi for #131124, 2022/01/12 reviewer:tianjy */
	/* repmgr对于超级用户判断，通过is_superuser判断，uxsmo该字段为off,
	 * 故被识别非超级用户，这块对uxsmo做特殊处理，当集群是安全功能时，
	 * 将uxsmo标记为超级用户，兼容模式时，UXSMO标记为超级用户 */
	/* Add by douwen for #181322, 2023/04/03 reviewer: huyn */
	if(current_user != NULL)
	{
		if(running_security_front &&
		   strcmp(current_user, running_mode_front == COMPATIBLE_MODE ? "UXSMO" : "uxsmo") == 0)
			is_superuser = true;

		if (userinfo != NULL)
		{
			snprintf(userinfo->username,
					sizeof(userinfo->username),
					"%s", current_user);
			userinfo->is_superuser = is_superuser;
		}
	}

	return is_superuser;
}


/* =============================== */
/* repmgrd shared memory functions */
/* =============================== */

bool
repmgrd_set_local_node_id(UXconn *conn, int local_node_id)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT repmgr.set_local_node_id(%i)",
					  local_node_id);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("repmgrd_set_local_node_id(): unable to execute query"));

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


int
repmgrd_get_local_node_id(UXconn *conn)
{
	UXresult   *res = NULL;
	int			local_node_id = UNKNOWN_NODE_ID;

	const char *sqlquery = "SELECT repmgr.get_local_node_id()";

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("repmgrd_get_local_node_id(): unable to execute query"));
	}
	else if (!UXSQLgetisnull(res, 0, 0))
	{
		local_node_id = atoi(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);

	return local_node_id;
}


bool
repmgrd_check_local_node_id(UXconn *conn)
{
	UXresult   *res = NULL;
	bool		node_id_settable = true;
	const char *sqlquery = "SELECT repmgr.get_local_node_id()";

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("repmgrd_get_local_node_id(): unable to execute query"));
	}

	if (UXSQLgetisnull(res, 0, 0))
	{
		node_id_settable = false;
	}

	UXSQLclear(res);

	return node_id_settable;
}


/*
 * Function that checks if the primary is in exclusive backup mode.
 * We'll use this when executing an action can conflict with an exclusive
 * backup.
 */
BackupState
server_in_exclusive_backup_mode(UXconn *conn)
{
	BackupState backup_state = BACKUP_STATE_UNKNOWN;
	const char *sqlquery = "SELECT ux_catalog.ux_is_in_backup()";
	UXresult   *res = NULL;

	/* Exclusive backup removed from UxsinoDB 15 */
	if (UXSQLserverVersion(conn) >= 150000)
		return BACKUP_STATE_NO_BACKUP;

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to retrieve information regarding backup mode of node"));

		backup_state = BACKUP_STATE_UNKNOWN;
	}
	else if (atobool(UXSQLgetvalue(res, 0, 0)) == true)
	{
		backup_state = BACKUP_STATE_IN_BACKUP;
	}
	else
	{
		backup_state = BACKUP_STATE_NO_BACKUP;
	}

	UXSQLclear(res);

	return backup_state;
}

void
repmgrd_set_pid(UXconn *conn, pid_t repmgrd_pid, const char *pidfile)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	log_verbose(LOG_DEBUG, "repmgrd_set_pid(): pid is %i", (int) repmgrd_pid);

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT repmgr.set_repmgrd_pid(%i, ",
					  (int) repmgrd_pid);

	if (pidfile != NULL)
	{
		appendUXSQLExpBuffer(&query,
						  " '%s')",
						  pidfile);
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 " NULL)");
	}

	res = UXSQLexec(conn, query.data);
	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.set_repmgrd_pid()\""));
		log_detail("%s", UXSQLerrorMessage(conn));
	}

	UXSQLclear(res);

	return;
}


pid_t
repmgrd_get_pid(UXconn *conn)
{
	UXresult   *res = NULL;
	pid_t		repmgrd_pid = UNKNOWN_PID;

	res = UXSQLexec(conn, "SELECT repmgr.get_repmgrd_pid()");

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.get_repmgrd_pid()\""));
		log_detail("%s", UXSQLerrorMessage(conn));
	}
	else if (!UXSQLgetisnull(res, 0, 0))
	{
		repmgrd_pid = atoi(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);

	return repmgrd_pid;
}


bool
repmgrd_is_running(UXconn *conn)
{
	UXresult   *res = NULL;
	bool		is_running = false;

	res = UXSQLexec(conn, "SELECT repmgr.repmgrd_is_running()");

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.repmgrd_is_running()\""));
		log_detail("%s", UXSQLerrorMessage(conn));
	}
	else if (!UXSQLgetisnull(res, 0, 0))
	{
		is_running = atobool(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);

	return is_running;
}


bool
repmgrd_is_paused(UXconn *conn)
{
	UXresult   *res = NULL;
	bool		is_paused = false;

	res = UXSQLexec(conn, "SELECT repmgr.repmgrd_is_paused()");

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.repmgrd_is_paused()\""));
		log_detail("%s", UXSQLerrorMessage(conn));
	}
	else if (!UXSQLgetisnull(res, 0, 0))
	{
		is_paused = atobool(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);

	return is_paused;
}


bool
repmgrd_pause(UXconn *conn, bool pause)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT repmgr.repmgrd_pause(%s)",
					  pause == true ? "TRUE" : "FALSE");
	res = UXSQLexec(conn, query.data);
	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to execute \"SELECT repmgr.repmgrd_pause()\""));
		log_detail("%s", UXSQLerrorMessage(conn));

		success = false;
	}

	UXSQLclear(res);

	return success;
}


int
repmgrd_get_upstream_node_id(UXconn *conn)
{
	UXresult   *res = NULL;
	int upstream_node_id = UNKNOWN_NODE_ID;

	const char *sqlquery = "SELECT repmgr.get_upstream_node_id()";

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("repmgrd_get_upstream_node_id(): unable to execute query"));
	}
	else if (!UXSQLgetisnull(res, 0, 0))
	{
		upstream_node_id = atoi(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);

	return upstream_node_id;
}


bool
repmgrd_set_upstream_node_id(UXconn *conn, int node_id)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  " SELECT repmgr.set_upstream_node_id(%i) ",
					  node_id);

	log_verbose(LOG_DEBUG, "repmgrd_set_upstream_node_id():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("repmgrd_set_upstream_node_id(): unable to set upstream node ID (provided value: %i)"), node_id);
		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}

/* ================ */
/* result functions */
/* ================ */

bool
atobool(const char *value)
{
	return (strcmp(value, "t") == 0)
		? true
		: false;
}


/* =================== */
/* extension functions */
/* =================== */

ExtensionStatus
get_repmgr_extension_status(UXconn *conn, t_extension_versions *extversions)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	ExtensionStatus status = REPMGR_UNKNOWN;

	/* TODO: check version */

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "	  SELECT ae.name, e.extname, "
						 "           ae.default_version, "
						 "           (((FLOOR(ae.default_version::NUMERIC)::INT) * 10000) + (ae.default_version::NUMERIC - FLOOR(ae.default_version::NUMERIC)::INT) * 1000)::INT AS available, "
						 "           ae.installed_version, "
						 "           (((FLOOR(ae.installed_version::NUMERIC)::INT) * 10000) + (ae.installed_version::NUMERIC - FLOOR(ae.installed_version::NUMERIC)::INT) * 1000)::INT AS installed "
						 "     FROM ux_catalog.ux_available_extensions ae "
						 "LEFT JOIN ux_catalog.ux_extension e "
						 "       ON e.extname=ae.name ");

	/* Modify by hc for #132965 at 2022/1/28 , reviewer:haoxg */
	/* ux_available_extensions适配兼容模式插件名为大写,这里需要同步适配 */
	if(running_mode_front == COMPATIBLE_MODE)
		appendUXSQLExpBufferStr(&query,
							"	   WHERE ae.name='REPMGR' ");
	else
		appendUXSQLExpBufferStr(&query,
						 "	   WHERE ae.name='repmgr' ");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_repmgr_extension_status(): unable to execute extension query"));
		status = REPMGR_UNKNOWN;
	}

	/* 1. Check extension is actually available */
	else if (UXSQLntuples(res) == 0)
	{
		status = REPMGR_UNAVAILABLE;
	}

	/* 2. Check if extension installed */
	else if (UXSQLgetisnull(res, 0, 1) == 0)
	{
		int available_version = atoi(UXSQLgetvalue(res, 0, 3));
		int installed_version = atoi(UXSQLgetvalue(res, 0, 5));

		/* caller wants to know which versions are installed/available */
		if (extversions != NULL)
		{
			snprintf(extversions->default_version,
					 sizeof(extversions->default_version),
					 "%s", UXSQLgetvalue(res, 0, 2));
			extversions->default_version_num = available_version;
			snprintf(extversions->installed_version,
					 sizeof(extversions->installed_version),
					 "%s", UXSQLgetvalue(res, 0, 4));
			extversions->installed_version_num = installed_version;
		}

		if (available_version > installed_version)
		{
			status = REPMGR_OLD_VERSION_INSTALLED;
		}
		else
		{
			status = REPMGR_INSTALLED;
		}
	}
	else
	{
		status = REPMGR_AVAILABLE;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return status;
}

/* ========================= */
/* node management functions */
/* ========================= */

/* assumes superuser connection */
void
checkpoint(UXconn *conn)
{
	UXresult   *res = NULL;

	res = UXSQLexec(conn, "CHECKPOINT");

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("unable to execute CHECKPOINT"));
	}

	UXSQLclear(res);
	return;
}


bool
vacuum_table(UXconn *primary_conn, const char *table)
{
	UXSQLExpBufferData query;
	bool		success = true;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query, "VACUUM %s", table);

	res = UXSQLexec(primary_conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(primary_conn, NULL,
					 _("unable to vacuum table \"%s\""), table);
		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}

/*
 * For use in UXsinoDB 12 and later
 */
bool
promote_standby(UXconn *conn, bool wait, int wait_seconds)
{
	UXSQLExpBufferData query;
	bool		success = true;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT ux_catalog.ux_promote(wait := %s",
					  wait ? "TRUE" : "FALSE");

	if (wait_seconds > 0)
	{
		appendUXSQLExpBuffer(&query,
						  ", wait_seconds := %i",
						  wait_seconds);
	}

	appendUXSQLExpBufferStr(&query, ")");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute ux_promote()"));
		success = false;
	}
	else
	{
		/* NOTE: if "wait" is false, ux_promote() will always return true */
		success = atobool(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


bool
resume_wal_replay(UXconn *conn)
{
	UXresult   *res = NULL;
	UXSQLExpBufferData query;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	if (UXSQLserverVersion(conn) >= 100000)
	{
		appendUXSQLExpBufferStr(&query,
							 "SELECT ux_catalog.ux_wal_replay_resume()");
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 "SELECT ux_catalog.ux_xlog_replay_resume()");
	}

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("resume_wal_replay(): unable to resume WAL replay"));
		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


/* ===================== */
/* Node record functions */
/* ===================== */

/*
 * Note: init_defaults may only be false when the caller is refreshing a previously
 * populated record.
 */
static RecordStatus
_get_node_record(UXconn *conn, char *sqlquery, t_node_info *node_info, bool init_defaults)
{
	int			ntuples = 0;
	UXresult   *res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("_get_node_record(): unable to execute query"));

		UXSQLclear(res);
		return RECORD_ERROR;
	}

	ntuples = UXSQLntuples(res);

	if (ntuples == 0)
	{
		UXSQLclear(res);
		return RECORD_NOT_FOUND;
	}

	_populate_node_record(res, node_info, 0, init_defaults);

	UXSQLclear(res);

	return RECORD_FOUND;
}


/*
 * Note: init_defaults may only be false when the caller is refreshing a previously
 * populated record.
 */
static void
_populate_node_record(UXresult *res, t_node_info *node_info, int row, bool init_defaults)
{
	node_info->node_id = atoi(UXSQLgetvalue(res, row, 0));
	node_info->type = parse_node_type(UXSQLgetvalue(res, row, 1));

	if (UXSQLgetisnull(res, row, 2))
	{
		node_info->upstream_node_id = NO_UPSTREAM_NODE;
	}
	else
	{
		node_info->upstream_node_id = atoi(UXSQLgetvalue(res, row, 2));
	}

	snprintf(node_info->node_name, sizeof(node_info->node_name), "%s", UXSQLgetvalue(res, row, 3));
	snprintf(node_info->conninfo, sizeof(node_info->conninfo), "%s", UXSQLgetvalue(res, row, 4));
	snprintf(node_info->repluser, sizeof(node_info->repluser), "%s", UXSQLgetvalue(res, row, 5));
	snprintf(node_info->slot_name, sizeof(node_info->slot_name), "%s", UXSQLgetvalue(res, row, 6));
	snprintf(node_info->location, sizeof(node_info->location), "%s", UXSQLgetvalue(res, row, 7));
	node_info->priority = atoi(UXSQLgetvalue(res, row, 8));
	node_info->active = atobool(UXSQLgetvalue(res, row, 9));
	snprintf(node_info->config_file, sizeof(node_info->config_file), "%s", UXSQLgetvalue(res, row, 10));

	/* These are only set by certain queries */
	snprintf(node_info->upstream_node_name, sizeof(node_info->upstream_node_name), "%s", UXSQLgetvalue(res, row, 11));

	if (UXSQLgetisnull(res, row, 14))
	{
		node_info->attached = NODE_ATTACHED_UNKNOWN;
	}
	else
	{
		node_info->attached = atobool(UXSQLgetvalue(res, row, 14)) ? NODE_ATTACHED : NODE_DETACHED;
	}

	/* Set remaining struct fields with default values */

	if (init_defaults == true)
	{
		node_info->node_status = NODE_STATUS_UNKNOWN;
		node_info->recovery_type = RECTYPE_UNKNOWN;
		node_info->last_wal_receive_lsn = InvalidXLogRecPtr;
		node_info->monitoring_state = MS_NORMAL;
		node_info->conn = NULL;
	}
}


t_server_type
parse_node_type(const char *type)
{
	if (strcmp(type, "primary") == 0)
	{
		return PRIMARY;
	}
	else if (strcmp(type, "standby") == 0)
	{
		return STANDBY;
	}
	else if (strcmp(type, "witness") == 0)
	{
		return WITNESS;
	}

	return UNKNOWN;
}


const char *
get_node_type_string(t_server_type type)
{
	switch (type)
	{
		case PRIMARY:
			return "primary";
		case STANDBY:
			return "standby";
		case WITNESS:
			return "witness";
			/* this should never happen */
		case UNKNOWN:
		default:
			log_error(_("unknown node type %i"), type);
			return "unknown";
	}
}


RecordStatus
get_node_record(UXconn *conn, int node_id, t_node_info *node_info)
{
	UXSQLExpBufferData query;
	RecordStatus result;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "SELECT " REPMGR_NODES_COLUMNS
					  "  FROM repmgr.nodes n "
					  " WHERE n.node_id = %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_node_record():\n  %s", query.data);

	result = _get_node_record(conn, query.data, node_info, true);
	termUXSQLExpBuffer(&query);

	if (result == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record(): no record found for node %i", node_id);
	}

	return result;
}


RecordStatus
refresh_node_record(UXconn *conn, int node_id, t_node_info *node_info)
{
	UXSQLExpBufferData query;
	RecordStatus result;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "SELECT " REPMGR_NODES_COLUMNS
					  "  FROM repmgr.nodes n "
					  " WHERE n.node_id = %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_node_record():\n  %s", query.data);

	result = _get_node_record(conn, query.data, node_info, false);
	termUXSQLExpBuffer(&query);

	if (result == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "refresh_node_record(): no record found for node %i", node_id);
	}

	return result;
}


RecordStatus
get_node_record_with_upstream(UXconn *conn, int node_id, t_node_info *node_info)
{
	UXSQLExpBufferData query;
	RecordStatus result;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
					  "    SELECT " REPMGR_NODES_COLUMNS_WITH_UPSTREAM
					  "      FROM repmgr.nodes n "
					  " LEFT JOIN repmgr.nodes un "
					  "        ON un.node_id = n.upstream_node_id"
					  " WHERE n.node_id = %i",
					  node_id);

	log_verbose(LOG_DEBUG, "get_node_record():\n  %s", query.data);

	result = _get_node_record(conn, query.data, node_info, true);
	termUXSQLExpBuffer(&query);

	if (result == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record(): no record found for node %i", node_id);
	}

	return result;
}


RecordStatus
get_node_record_by_name(UXconn *conn, const char *node_name, t_node_info *node_info)
{
	UXSQLExpBufferData query;
	RecordStatus record_status = RECORD_NOT_FOUND;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT " REPMGR_NODES_COLUMNS
					  "  FROM repmgr.nodes n "
					  " WHERE n.node_name = '%s' ",
					  node_name);

	log_verbose(LOG_DEBUG, "get_node_record_by_name():\n  %s", query.data);

	record_status = _get_node_record(conn, query.data, node_info, true);

	termUXSQLExpBuffer(&query);

	if (record_status == RECORD_NOT_FOUND)
	{
		log_verbose(LOG_DEBUG, "get_node_record_by_name(): no record found for node \"%s\"",
					node_name);
	}

	return record_status;
}


t_node_info *
get_node_record_pointer(UXconn *conn, int node_id)
{
	t_node_info *node_info = ux_malloc0(sizeof(t_node_info));
	RecordStatus record_status = RECORD_NOT_FOUND;

	record_status = get_node_record(conn, node_id, node_info);

	if (record_status != RECORD_FOUND)
	{
		pfree(node_info);
		return NULL;
	}

	return node_info;
}


bool
get_primary_node_record(UXconn *conn, t_node_info *node_info)
{
	RecordStatus record_status = RECORD_NOT_FOUND;

	int			primary_node_id = get_primary_node_id(conn);

	if (primary_node_id == UNKNOWN_NODE_ID)
	{
		return false;
	}

	record_status = get_node_record(conn, primary_node_id, node_info);

	return record_status == RECORD_FOUND ? true : false;
}


/*
 * Get the local node record; if this fails, exit. Many operations
 * depend on this being available, so we'll centralize the check
 * and failure messages here.
 */
bool
get_local_node_record(UXconn *conn, int node_id, t_node_info *node_info)
{
	RecordStatus record_status = get_node_record(conn, node_id, node_info);

	if (record_status != RECORD_FOUND)
	{
		log_error(_("unable to retrieve record for local node"));
		log_detail(_("local node id is  %i"), node_id);
		log_hint(_("check this node was correctly registered"));

		UXSQLfinish(conn);
		exit(ERR_BAD_CONFIG);
	}

	return true;
}


static
void
_populate_node_records(UXresult *res, NodeInfoList *node_list)
{
	int			i;

	clear_node_info_list(node_list);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		return;
	}

	for (i = 0; i < UXSQLntuples(res); i++)
	{
		NodeInfoListCell *cell;

		cell = (NodeInfoListCell *) ux_malloc0(sizeof(NodeInfoListCell));

		cell->node_info = ux_malloc0(sizeof(t_node_info));

		_populate_node_record(res, cell->node_info, i, true);

		if (node_list->tail)
			node_list->tail->next = cell;
		else
			node_list->head = cell;

		node_list->tail = cell;
		node_list->node_count++;
	}

	return;
}


bool
get_all_node_records(UXconn *conn, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool success = true;
	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "  SELECT " REPMGR_NODES_COLUMNS
						 "    FROM repmgr.nodes n "
						 "ORDER BY n.node_id ");

	log_verbose(LOG_DEBUG, "get_all_node_records():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_all_node_records(): unable to execute query"));
		success = false;
	}

	UXSQLclear(res);
	termUXSQLExpBuffer(&query);

	return success;
}

/*
 * uxdb db added
 * In primary monitoring loop, read all nodes and use the info
 * for auto node rejoin. this function would be called by the
 * end of the loop, if query data failed, keep the old info
 * instead of clear the node_list
 */
void
ux_get_all_node_records(UXconn *conn, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
			"  SELECT " REPMGR_NODES_COLUMNS
			"    FROM repmgr.nodes n "
			"ORDER BY n.node_id ");

	log_verbose(LOG_DEBUG, "ux_get_all_node_records():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) == UXRES_TUPLES_OK)
	{
		_populate_node_records(res, node_list);
	}

	UXSQLclear(res);

	return;
}

bool
get_all_nodes_count(UXconn *conn, int *count)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool success = true;
	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "  SELECT count(*) "
						 "    FROM repmgr.nodes n ");

	log_verbose(LOG_DEBUG, "get_all_nodes_count():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_all_nodes_count(): unable to execute query"));
		success = false;
	}
	else
	{
		*count = atoi(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);
	termUXSQLExpBuffer(&query);

	return success;
}

void
get_downstream_node_records(UXconn *conn, int node_id, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "  SELECT " REPMGR_NODES_COLUMNS
					  "    FROM repmgr.nodes n "
					  "   WHERE n.upstream_node_id = %i "
					  "ORDER BY n.node_id ",
					  node_id);

	log_verbose(LOG_DEBUG, "get_downstream_node_records():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_downstream_node_records(): unable to execute query"));
	}

	termUXSQLExpBuffer(&query);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	UXSQLclear(res);

	return;
}


void
get_active_sibling_node_records(UXconn *conn, int node_id, int upstream_node_id, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "  SELECT " REPMGR_NODES_COLUMNS
					  "    FROM repmgr.nodes n "
					  "   WHERE n.upstream_node_id = %i "
					  "     AND n.node_id != %i "
					  "     AND n.active IS TRUE "
					  "ORDER BY n.node_id ",
					  upstream_node_id,
					  node_id);

	log_verbose(LOG_DEBUG, "get_active_sibling_node_records():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_active_sibling_records(): unable to execute query"));
	}

	termUXSQLExpBuffer(&query);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	UXSQLclear(res);

	return;
}


bool
get_child_nodes(UXconn *conn, int node_id, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "    SELECT n.node_id, n.type, n.upstream_node_id, n.node_name, n.conninfo, n.repluser, "
					  "           n.slot_name, n.location, n.priority, n.active, n.config_file, "
					  "           '' AS upstream_node_name, n.uxdb_passwd, n.root_passwd, "
					  "           CASE WHEN sr.application_name IS NULL THEN FALSE ELSE TRUE END AS attached "
					  "      FROM repmgr.nodes n "
					  " LEFT JOIN ux_catalog.ux_stat_replication sr "
					  "        ON sr.application_name = n.node_name "
					  "     WHERE n.upstream_node_id = %i ",
					  node_id);

	log_verbose(LOG_DEBUG, "get_child_nodes():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_child_nodes(): unable to execute query"));
		success = false;
	}

	termUXSQLExpBuffer(&query);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	UXSQLclear(res);

	return success;
}


void
get_node_records_by_priority(UXconn *conn, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "  SELECT " REPMGR_NODES_COLUMNS
						 "    FROM repmgr.nodes n "
						 "ORDER BY n.priority DESC, n.node_name ");

	log_verbose(LOG_DEBUG, "get_node_records_by_priority():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_node_records_by_priority(): unable to execute query"));
	}

	termUXSQLExpBuffer(&query);

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	UXSQLclear(res);

	return;
}

/*
 * return all node records together with their upstream's node name,
 * if available.
 */
bool
get_all_node_records_with_upstream(UXconn *conn, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "    SELECT " REPMGR_NODES_COLUMNS_WITH_UPSTREAM
						 "      FROM repmgr.nodes n "
						 " LEFT JOIN repmgr.nodes un "
						 "        ON un.node_id = n.upstream_node_id"
						 "  ORDER BY n.node_id ");

	log_verbose(LOG_DEBUG, "get_all_node_records_with_upstream():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_all_node_records_with_upstream(): unable to retrieve node records"));
		success = false;
	}


	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}



bool
get_downstream_nodes_with_missing_slot(UXconn *conn, int this_node_id, NodeInfoList *node_list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "   SELECT " REPMGR_NODES_COLUMNS
					  "     FROM repmgr.nodes n "
					  "LEFT JOIN ux_catalog.ux_replication_slots rs "
					  "       ON rs.slot_name = n.slot_name "
					  "    WHERE n.slot_name IS NOT NULL"
					  "      AND rs.slot_name IS NULL "
					  "      AND n.upstream_node_id = %i "
					  "      AND n.type = 'standby'",
					  this_node_id);

	log_verbose(LOG_DEBUG, "get_all_node_records_with_missing_slot():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("get_downstream_nodes_with_missing_slot(): unable to retrieve node records"));
		success = false;
	}

	/* this will return an empty list if there was an error executing the query */
	_populate_node_records(res, node_list);

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}

bool
create_node_record(UXconn *conn, char *repmgr_action, t_node_info *node_info)
{
	if (repmgr_action != NULL)
		log_verbose(LOG_DEBUG, "create_node_record(): action is \"%s\"", repmgr_action);

	return _create_update_node_record(conn, "create", node_info);
}


bool
update_node_record(UXconn *conn, char *repmgr_action, t_node_info *node_info)
{
	if (repmgr_action != NULL)
		log_verbose(LOG_DEBUG, "update_node_record(): action is \"%s\"", repmgr_action);

	return _create_update_node_record(conn, "update", node_info);
}


static bool
_create_update_node_record(UXconn *conn, char *action, t_node_info *node_info)
{
	UXSQLExpBufferData query;
	char		node_id[MAXLEN] = "";
	char		priority[MAXLEN] = "";

	char		upstream_node_id[MAXLEN] = "";
	char	   *upstream_node_id_ptr = NULL;

	char	   *slot_name_ptr = NULL;

	int			param_count = NODE_RECORD_PARAM_COUNT;
	const char *param_values[NODE_RECORD_PARAM_COUNT];

	UXresult   *res;
	bool		success = true;

	maxlen_snprintf(node_id, "%i", node_info->node_id);
	maxlen_snprintf(priority, "%i", node_info->priority);

	if (node_info->upstream_node_id == NO_UPSTREAM_NODE && node_info->type == STANDBY)
	{
		/*
		 * No explicit upstream node id provided for standby - attempt to get
		 * primary node id
		 */
		int			primary_node_id = get_primary_node_id(conn);

		maxlen_snprintf(upstream_node_id, "%i", primary_node_id);
		upstream_node_id_ptr = upstream_node_id;
	}
	else if (node_info->upstream_node_id != NO_UPSTREAM_NODE)
	{
		maxlen_snprintf(upstream_node_id, "%i", node_info->upstream_node_id);
		upstream_node_id_ptr = upstream_node_id;
	}

	if (node_info->slot_name[0] != '\0')
	{
		slot_name_ptr = node_info->slot_name;
	}


	param_values[0] = get_node_type_string(node_info->type);
	param_values[1] = upstream_node_id_ptr;
	param_values[2] = node_info->node_name;
	param_values[3] = node_info->conninfo;
	param_values[4] = node_info->repluser;
	param_values[5] = slot_name_ptr;
	param_values[6] = node_info->location;
	param_values[7] = priority;
	param_values[8] = node_info->active == true ? "TRUE" : "FALSE";
	param_values[9] = node_info->config_file;
	param_values[10] = node_id;
	param_values[11] = node_info->virtual_ip;   //uxdb
	param_values[12] = node_info->network_card; //uxdb
	/* add by songjinzhou for #178952 at 2023/03/16 reveiwer houjiaxing. */
	param_values[13] = node_info->uxdb_passwd;
	param_values[14] = node_info->root_passwd;

	initUXSQLExpBuffer(&query);

	if (strcmp(action, "create") == 0)
	{
		appendUXSQLExpBufferStr(&query,
							 "INSERT INTO repmgr.nodes "
							 "       (node_id, type, upstream_node_id, "
							 "        node_name, conninfo, repluser, slot_name, "
							 "        location, priority, active, config_file, virtual_ip, network_card) "
							 "VALUES ($11, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $12, $13) ");
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 "UPDATE repmgr.nodes SET "
							 "       type = $1, "
							 "       upstream_node_id = $2, "
							 "       node_name = $3, "
							 "       conninfo = $4, "
							 "       repluser = $5, "
							 "       slot_name = $6, "
							 "       location = $7, "
							 "       priority = $8, "
							 "       active = $9, "
							 "       config_file = $10, "
							 "       virtual_ip = $12, "
							 "       network_card = $13, "
							 "       uxdb_passwd = $14, "
							 "       root_passwd = $15 "
							 " WHERE node_id = $11 ");
	}

	res = UXSQLexecParams(conn,
					   query.data,
					   param_count,
					   NULL,
					   param_values,
					   NULL,
					   NULL,
					   0);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("_create_update_node_record(): unable to %s node record for node \"%s\" (ID: %i)"),
					 action,
					 node_info->node_name,
					 node_info->node_id);

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


bool
update_node_record_set_active(UXconn *conn, int this_node_id, bool active)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "UPDATE repmgr.nodes SET active = %s "
					  " WHERE node_id = %i",
					  active == true ? "TRUE" : "FALSE",
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_active():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("update_node_record_set_active(): unable to update node record"));
		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


bool
update_node_record_set_active_standby(UXconn *conn, int this_node_id)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "UPDATE repmgr.nodes "
					  "   SET type = 'standby', "
					  "       active = TRUE "
					  " WHERE node_id = %i",
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_active_standby():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("update_node_record_set_active_standby(): unable to update node record"));
		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


bool
update_node_record_set_primary(UXconn *conn, int this_node_id)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	log_debug(_("setting node %i as primary and marking existing primary as failed"),
			  this_node_id);

	begin_transaction(conn);

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET active = FALSE "
					  "   WHERE type = 'primary' "
					  "     AND active IS TRUE "
					  "     AND node_id != %i ",
					  this_node_id);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("update_node_record_set_primary(): unable to set old primary node as inactive"));

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		rollback_transaction(conn);

		return false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "  UPDATE repmgr.nodes"
					  "     SET type = 'primary', "
					  "         upstream_node_id = NULL, "
					  "         active = TRUE "
					  "   WHERE node_id = %i ",
					  this_node_id);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("unable to set current node %i as active primary"),
					 this_node_id);

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		rollback_transaction(conn);

		return false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return commit_transaction(conn);
}


bool
update_node_record_set_upstream(UXconn *conn, int this_node_id, int new_upstream_node_id)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	log_debug(_("update_node_record_set_upstream(): Updating node %i's upstream node to %i"),
			  this_node_id, new_upstream_node_id);

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET upstream_node_id = %i "
					  "   WHERE node_id = %i ",
					  new_upstream_node_id,
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_set_upstream():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("update_node_record_set_upstream(): unable to set new upstream node id"));

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


/*
 * Update node record following change of status
 * (e.g. inactive primary converted to standby)
 */
bool
update_node_record_status(UXconn *conn, int this_node_id, char *type, int upstream_node_id, bool active)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "  UPDATE repmgr.nodes "
					  "     SET type = '%s', "
					  "         upstream_node_id = %i, "
					  "         active = %s "
					  "   WHERE node_id = %i ",
					  type,
					  upstream_node_id,
					  active ? "TRUE" : "FALSE",
					  this_node_id);

	log_verbose(LOG_DEBUG, "update_node_record_status():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data,
					 _("update_node_record_status(): unable to update node record status for node %i"),
					 this_node_id);

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


/*
 * Update node record's "conninfo" and "priority" fields. Called by repmgrd
 * following a configuration file reload.
 */
bool
update_node_record_conn_priority(UXconn *conn, t_configuration_options *options)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "UPDATE repmgr.nodes "
					  "   SET conninfo = '%s', "
					  "       priority = %d "
					  " WHERE node_id = %d ",
					  options->conninfo,
					  options->priority,
					  options->node_id);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("update_node_record_conn_priority(): unable to execute query"));
		success = false;
	}

	termUXSQLExpBuffer(&query);

	UXSQLclear(res);

	return success;
}


/*
 * Copy node records from primary to witness servers.
 *
 * This is used when initially registering a witness server, and
 * by repmgrd to update the node records when required.
 */

bool
witness_copy_node_records(UXconn *primary_conn, UXconn *witness_conn)
{
	UXresult   *res = NULL;
	NodeInfoList nodes = T_NODE_INFO_LIST_INITIALIZER;
	NodeInfoListCell *cell = NULL;

	begin_transaction(witness_conn);

	/* Defer constraints */

	res = UXSQLexec(witness_conn, "SET CONSTRAINTS ALL DEFERRED");

	if (!res || UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(witness_conn, NULL, ("witness_copy_node_records(): unable to defer constraints"));

		rollback_transaction(witness_conn);
		UXSQLclear(res);

		return false;
	}

	UXSQLclear(res);

	/* truncate existing records */

	if (truncate_node_records(witness_conn) == false)
	{
		rollback_transaction(witness_conn);

		return false;
	}

	if (get_all_node_records(primary_conn, &nodes) == false)
	{
		rollback_transaction(witness_conn);

		return false;
	}

	for (cell = nodes.head; cell; cell = cell->next)
	{
		if (create_node_record(witness_conn, NULL, cell->node_info) == false)
		{
			rollback_transaction(witness_conn);

			return false;
		}
	}

	/* and done */
	commit_transaction(witness_conn);

	clear_node_info_list(&nodes);

	return true;
}


bool
delete_node_record(UXconn *conn, int node)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "DELETE FROM repmgr.nodes "
					  " WHERE node_id = %i",
					  node);

	log_verbose(LOG_DEBUG, "delete_node_record():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("delete_node_record(): unable to delete node record"));

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}

bool
truncate_node_records(UXconn *conn)
{
	UXresult   *res = NULL;
	bool		success = true;

	res = UXSQLexec(conn, "TRUNCATE TABLE repmgr.nodes");

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("truncate_node_records(): unable to truncate table \"repmgr.nodes\""));

		success = false;
	}

	UXSQLclear(res);

	return success;
}


bool
update_node_record_slot_name(UXconn *primary_conn, int node_id, char *slot_name)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  " UPDATE repmgr.nodes "
					  "    SET slot_name = '%s' "
					  "  WHERE node_id = %i ",
					  slot_name,
					  node_id);

	res = UXSQLexec(primary_conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(primary_conn, query.data, _("unable to set node record slot name"));

		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}




void
clear_node_info_list(NodeInfoList *nodes)
{
	NodeInfoListCell *cell = NULL;
	NodeInfoListCell *next_cell = NULL;

	log_verbose(LOG_DEBUG, "clear_node_info_list() - closing open connections");

	/* close any open connections */
	for (cell = nodes->head; cell; cell = cell->next)
	{

		if (UXSQLstatus(cell->node_info->conn) == CONNECTION_OK)
		{
			UXSQLfinish(cell->node_info->conn);
			cell->node_info->conn = NULL;
		}
	}

	log_verbose(LOG_DEBUG, "clear_node_info_list() - unlinking");

	cell = nodes->head;

	while (cell != NULL)
	{
		next_cell = cell->next;

		if (cell->node_info->replication_info != NULL)
			pfree(cell->node_info->replication_info);

		pfree(cell->node_info);
		pfree(cell);
		cell = next_cell;
	}

	nodes->head = NULL;
	nodes->tail = NULL;
	nodes->node_count = 0;
}


/* ================================================ */
/* UXsinoDB configuration file location functions */
/* ================================================ */

bool
get_datadir_configuration_files(UXconn *conn, KeyValueList *list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			i;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "WITH files AS ( "
						 "  WITH dd AS ( "
						 "   SELECT setting "
						 "     FROM ux_catalog.ux_settings "
						 "    WHERE name = 'data_directory') "
						 "   SELECT distinct(sourcefile) AS config_file"
						 "     FROM dd, ux_catalog.ux_settings ps "
						 "    WHERE ps.sourcefile IS NOT NULL "
						 "      AND ps.sourcefile ~ (ux_catalog.concat('^', dd.setting)) "
						 "       UNION "
						 "   SELECT ps.setting  AS config_file"
						 "     FROM dd, ux_catalog.ux_settings ps "
						 "    WHERE ps.name IN ('config_file', 'hba_file', 'ident_file') "
						 "      AND ps.setting ~ (ux_catalog.concat('^', dd.setting)) "
						 ") "
						 "  SELECT config_file, "
						 "         ux_catalog.regexp_replace(config_file, '^.*\\/','') AS filename "
						 "    FROM files "
						 "ORDER BY config_file");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_datadir_configuration_files(): unable to retrieve configuration file information"));

		success = false;
	}
	else
	{
		for (i = 0; i < UXSQLntuples(res); i++)
		{
			key_value_list_set(list,
							   UXSQLgetvalue(res, i, 1),
							   UXSQLgetvalue(res, i, 0));
		}
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


bool
get_configuration_file_locations(UXconn *conn, t_configfile_list *list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			i;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "  WITH dd AS ( "
						 "    SELECT setting AS data_directory"
						 "      FROM ux_catalog.ux_settings "
						 "     WHERE name = 'data_directory' "
						 "  ) "
						 "    SELECT DISTINCT(sourcefile), "
						 "           ux_catalog.regexp_replace(sourcefile, '^.*\\/', '') AS filename, "
						 "           sourcefile ~ (ux_catalog.concat('^', dd.data_directory)) AS in_data_dir "
						 "      FROM dd, ux_catalog.ux_settings ps "
						 "     WHERE sourcefile IS NOT NULL "
						 "  ORDER BY 1 ");

	log_verbose(LOG_DEBUG, "get_configuration_file_locations():\n  %s",
				query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_configuration_file_locations(): unable to retrieve configuration file locations"));

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		return false;
	}

	/*
	 * allocate memory for config file array - number of rows returned from
	 * above query + 2 for ux_hba.conf, ux_ident.conf
	 */

	config_file_list_init(list, UXSQLntuples(res) + 2);

	for (i = 0; i < UXSQLntuples(res); i++)
	{
		config_file_list_add(list,
							 UXSQLgetvalue(res, i, 0),
							 UXSQLgetvalue(res, i, 1),
							 atobool(UXSQLgetvalue(res, i, 2)));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	/* Fetch locations of ux_hba.conf and ux_ident.conf */
	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "  WITH dd AS ( "
						 "    SELECT setting AS data_directory"
						 "      FROM ux_catalog.ux_settings "
						 "     WHERE name = 'data_directory' "
						 "  ) "
						 "    SELECT ps.setting, "
						 "           ux_catalog.regexp_replace(setting, '^.*\\/', '') AS filename, "
						 "           ps.setting ~ (ux_catalog.concat('^', dd.data_directory)) AS in_data_dir "
						 "      FROM dd, ux_catalog.ux_settings ps "
						 "     WHERE ps.name IN ('hba_file', 'ident_file') "
						 "  ORDER BY 1 ");

	log_verbose(LOG_DEBUG, "get_configuration_file_locations():\n  %s",
				query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_configuration_file_locations(): unable to retrieve configuration file locations"));

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		return false;
	}

	for (i = 0; i < UXSQLntuples(res); i++)
	{
		config_file_list_add(list,
							 UXSQLgetvalue(res, i, 0),
							 UXSQLgetvalue(res, i, 1),
							 atobool(UXSQLgetvalue(res, i, 2)));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return true;
}


void
config_file_list_init(t_configfile_list *list, int max_size)
{
	list->size = max_size;
	list->entries = 0;
	list->files = ux_malloc0(sizeof(t_configfile_info *) * max_size);

	if (list->files == NULL)
	{
		log_error(_("config_file_list_init(): unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}
}


void
config_file_list_add(t_configfile_list *list, const char *file, const char *filename, bool in_data_dir)
{
	/* Failsafe to prevent entries being added beyond the end */
	if (list->entries == list->size)
		return;

	list->files[list->entries] = ux_malloc0(sizeof(t_configfile_info));

	if (list->files[list->entries] == NULL)
	{
		log_error(_("config_file_list_add(): unable to allocate memory; terminating"));
		exit(ERR_OUT_OF_MEMORY);
	}


	snprintf(list->files[list->entries]->filepath,
			 sizeof(list->files[list->entries]->filepath),
			 "%s", file);
	canonicalize_path(list->files[list->entries]->filepath);

	snprintf(list->files[list->entries]->filename,
			 sizeof(list->files[list->entries]->filename),
			 "%s", filename);

	list->files[list->entries]->in_data_directory = in_data_dir;

	list->entries++;
}


/* ====================== */
/* event record functions */
/* ====================== */


/*
 * create_event_record()
 *
 * Create a record in the "events" table, but don't execute the
 * "event_notification_command".
 */

bool
create_event_record(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details)
{
	/* create dummy t_event_info */
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	return _create_event(conn, options, node_id, event, successful, details, &event_info, false);
}


/*
 * create_event_notification()
 *
 * If `conn` is not NULL, insert a record into the events table.
 *
 * If configuration parameter "event_notification_command" is set, also
 * attempt to execute that command.
 *
 * Returns true if all operations succeeded, false if one or more failed.
 *
 * Note this function may be called with "conn" set to NULL in cases where
 * the primary node is not available and it's therefore not possible to write
 * an event record. In this case, if `event_notification_command` is set, a
 * user-defined notification to be generated; if not, this function will have
 * no effect.
 */
bool
create_event_notification(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details)
{
	/* create dummy t_event_info */
	t_event_info event_info = T_EVENT_INFO_INITIALIZER;

	return _create_event(conn, options, node_id, event, successful, details, &event_info, true);
}


/*
 * create_event_notification_extended()
 *
 * The caller may need to pass additional parameters to the event notification
 * command (currently only the conninfo string of another node)

 */
bool
create_event_notification_extended(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info)
{
	return _create_event(conn, options, node_id, event, successful, details, event_info, true);
}


static bool
_create_event(UXconn *conn, t_configuration_options *options, int node_id, char *event, bool successful, char *details, t_event_info *event_info, bool send_notification)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	char		event_timestamp[MAXLEN] = "";
	bool		success = true;

	log_verbose(LOG_DEBUG, "_create_event(): event is \"%s\" for node %i", event, node_id);

	/*
	 * Only attempt to write a record if a connection handle was provided,
	 * and the connection handle points to a node which is not in recovery.
	 */
	if (conn != NULL && UXSQLstatus(conn) == CONNECTION_OK && get_recovery_type(conn) == RECTYPE_PRIMARY)
	{
		int			n_node_id = htonl(node_id);
		char	   *t_successful = successful ? "TRUE" : "FALSE";

		const char *values[4] = {(char *) &n_node_id,
			event,
			t_successful,
			details
		};

		int			lengths[4] = {sizeof(n_node_id),
			0,
			0,
			0
		};

		int			binary[4] = {1, 0, 0, 0};

		initUXSQLExpBuffer(&query);
		appendUXSQLExpBufferStr(&query,
							 " INSERT INTO repmgr.events ( "
							 "             node_id, "
							 "             event, "
							 "             successful, "
							 "             details "
							 "            ) "
							 "      VALUES ($1, $2, $3, $4) "
							 "   RETURNING event_timestamp ");

		log_verbose(LOG_DEBUG, "_create_event():\n  %s", query.data);

		res = UXSQLexecParams(conn,
						   query.data,
						   4,
						   NULL,
						   values,
						   lengths,
						   binary,
						   0);


		if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
		{
			/* we don't treat this as a fatal error */
			log_warning(_("unable to create event record"));
			log_detail("%s", UXSQLerrorMessage(conn));
			log_detail("%s", query.data);

			success = false;
		}
		else
		{
			/* Store timestamp to send to the notification command */
			snprintf(event_timestamp, MAXLEN, "%s", UXSQLgetvalue(res, 0, 0));
		}

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);
	}

	/*
	 * If no database connection provided, or the query failed, generate a
	 * current timestamp ourselves. This isn't quite the same format as
	 * UXsinoDB, but is close enough for diagnostic use.
	 */
	if (!strlen(event_timestamp))
	{
		time_t		now;
		struct tm	ts;

		time(&now);
		ts = *localtime(&now);
		strftime(event_timestamp, MAXLEN, "%Y-%m-%d %H:%M:%S%z", &ts);
	}

	log_verbose(LOG_DEBUG, "_create_event(): Event timestamp is \"%s\"", event_timestamp);

	/* an event notification command was provided - parse and execute it */
	if (send_notification == true && strlen(options->event_notification_command))
	{
		char		parsed_command[MAXUXPATH] = "";
		const char *src_ptr = NULL;
		char	   *dst_ptr = NULL;
		char	   *end_ptr = NULL;
		int			r = 0;

		log_verbose(LOG_DEBUG, "_create_event(): command is '%s'", options->event_notification_command);
		/*
		 * If configuration option 'event_notifications' was provided, check
		 * if this event is one of the ones listed; if not listed, don't
		 * execute the notification script.
		 *
		 * (If 'event_notifications' was not provided, we assume the script
		 * should be executed for all events).
		 */
		if (options->event_notifications.head != NULL)
		{
			EventNotificationListCell *cell = NULL;
			bool		notify_ok = false;

			for (cell = options->event_notifications.head; cell; cell = cell->next)
			{
				if (strcmp(event, cell->event_type) == 0)
				{
					notify_ok = true;
					break;
				}
			}

			/*
			 * Event type not found in the 'event_notifications' list - return
			 * early
			 */
			if (notify_ok == false)
			{
				log_debug(_("not executing notification script for event type \"%s\""), event);
				return success;
			}
		}

		dst_ptr = parsed_command;
		end_ptr = parsed_command + MAXUXPATH - 1;
		*end_ptr = '\0';

		for (src_ptr = options->event_notification_command; *src_ptr; src_ptr++)
		{
			if (*src_ptr == '%')
			{
				switch (src_ptr[1])
				{
					case '%':
						/* %%: replace with % */
						if (dst_ptr < end_ptr)
						{
							src_ptr++;
							*dst_ptr++ = *src_ptr;
						}
						break;
					case 'n':
						/* %n: node id */
						src_ptr++;
						snprintf(dst_ptr, end_ptr - dst_ptr, "%i", node_id);
						dst_ptr += strlen(dst_ptr);
						break;
					case 'a':
						/* %a: node name */
						src_ptr++;
						if (event_info->node_name != NULL)
						{
							log_verbose(LOG_DEBUG, "node_name: %s", event_info->node_name);
							strlcpy(dst_ptr, event_info->node_name, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
						}
						break;
					case 'e':
						/* %e: event type */
						src_ptr++;
						strlcpy(dst_ptr, event, end_ptr - dst_ptr);
						dst_ptr += strlen(dst_ptr);
						break;
					case 'd':
						/* %d: details */
						src_ptr++;
						if (details != NULL)
						{
							UXSQLExpBufferData details_escaped;
							initUXSQLExpBuffer(&details_escaped);

							escape_double_quotes(details, &details_escaped);

							strlcpy(dst_ptr, details_escaped.data, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
							termUXSQLExpBuffer(&details_escaped);
						}
						break;
					case 's':
						/* %s: successful */
						src_ptr++;
						strlcpy(dst_ptr, successful ? "1" : "0", end_ptr - dst_ptr);
						dst_ptr += strlen(dst_ptr);
						break;
					case 't':
						/* %t: timestamp */
						src_ptr++;
						strlcpy(dst_ptr, event_timestamp, end_ptr - dst_ptr);
						dst_ptr += strlen(dst_ptr);
						break;
					case 'c':
						/* %c: conninfo for next available node */
						src_ptr++;
						if (event_info->conninfo_str != NULL)
						{
							log_debug("conninfo: %s", event_info->conninfo_str);

							strlcpy(dst_ptr, event_info->conninfo_str, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
						}
						break;
					case 'p':
						/* %p: primary id ("standby_switchover"/"repmgrd_failover_promote": former primary id) */
						src_ptr++;
						if (event_info->node_id != UNKNOWN_NODE_ID)
						{
							UXSQLExpBufferData node_id;
							initUXSQLExpBuffer(&node_id);
							appendUXSQLExpBuffer(&node_id,
											  "%i", event_info->node_id);
							strlcpy(dst_ptr, node_id.data, end_ptr - dst_ptr);
							dst_ptr += strlen(dst_ptr);
							termUXSQLExpBuffer(&node_id);
						}
						break;
					default:
						/* otherwise treat the % as not special */
						if (dst_ptr < end_ptr)
							*dst_ptr++ = *src_ptr;
						break;
				}
			}
			else
			{
				if (dst_ptr < end_ptr)
					*dst_ptr++ = *src_ptr;
			}
		}

		*dst_ptr = '\0';

		log_info(_("executing notification command for event \"%s\""),
				 event);

		log_detail(_("command is:\n  %s"), parsed_command);
		r = ux_system(parsed_command);
		if (r != 0)
		{
			log_warning(_("unable to execute event notification command"));
			log_detail(_("parsed event notification command was:\n  %s"), parsed_command);
			success = false;
		}
	}

	return success;
}


UXresult *
get_event_records(UXconn *conn, int node_id, const char *node_name, const char *event, bool all, int limit)
{
	UXresult   *res;

	UXSQLExpBufferData query;
	UXSQLExpBufferData where_clause;


	initUXSQLExpBuffer(&query);
	initUXSQLExpBuffer(&where_clause);

	/* LEFT JOIN used here as a node record may have been removed */
	appendUXSQLExpBufferStr(&query,
						 "   SELECT e.node_id, n.node_name, e.event, e.successful, "
						 "          ux_catalog.to_char(e.event_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS timestamp, "
						 "          e.details "
						 "     FROM repmgr.events e "
						 "LEFT JOIN repmgr.nodes n ON e.node_id = n.node_id ");

	if (node_id != UNKNOWN_NODE_ID)
	{
		append_where_clause(&where_clause,
							"n.node_id=%i", node_id);
	}
	else if (node_name[0] != '\0')
	{
		char	   *escaped = escape_string(conn, node_name);

		if (escaped == NULL)
		{
			log_error(_("unable to escape value provided for node name"));
			log_detail(_("node name is: \"%s\""), node_name);
		}
		else
		{
			append_where_clause(&where_clause,
								"n.node_name='%s'",
								escaped);
			pfree(escaped);
		}
	}

	if (event[0] != '\0')
	{
		char	   *escaped = escape_string(conn, event);

		if (escaped == NULL)
		{
			log_error(_("unable to escape value provided for event"));
			log_detail(_("event is: \"%s\""), event);
		}
		else
		{
			append_where_clause(&where_clause,
								"e.event='%s'",
								escaped);
			pfree(escaped);
		}
	}

	appendUXSQLExpBuffer(&query, "\n%s\n",
					  where_clause.data);

	appendUXSQLExpBufferStr(&query,
						 " ORDER BY e.event_timestamp DESC");

	if (all == false && limit > 0)
	{
		appendUXSQLExpBuffer(&query, " LIMIT %i",
						  limit);
	}

	log_debug("do_cluster_event():\n%s", query.data);
	res = UXSQLexec(conn, query.data);

	termUXSQLExpBuffer(&query);
	termUXSQLExpBuffer(&where_clause);

	return res;
}


/* ========================== */
/* replication slot functions */
/* ========================== */


void
create_slot_name(char *slot_name, int node_id)
{
	maxlen_snprintf(slot_name, "repmgr_slot_%i", node_id);
}


static ReplSlotStatus
_verify_replication_slot(UXconn *conn, char *slot_name, UXSQLExpBufferData *error_msg)
{
	RecordStatus record_status = RECORD_NOT_FOUND;
	t_replication_slot slot_info = T_REPLICATION_SLOT_INITIALIZER;

	/*
	 * Check whether slot exists already; if it exists and is active, that
	 * means another active standby is using it, which creates an error
	 * situation; if not we can reuse it as-is.
	 */
	record_status = get_slot_record(conn, slot_name, &slot_info);

	if (record_status == RECORD_FOUND)
	{
		if (strcmp(slot_info.slot_type, "physical") != 0)
		{
			if (error_msg)
				appendUXSQLExpBuffer(error_msg,
								  _("slot \"%s\" exists and is not a physical slot\n"),
								  slot_name);
			return SLOT_NOT_PHYSICAL;
		}

		if (slot_info.active == false)
		{
			log_debug("replication slot \"%s\" exists but is inactive; reusing",
					  slot_name);

			return SLOT_INACTIVE;
		}

		if (error_msg)
			appendUXSQLExpBuffer(error_msg,
							  _("slot \"%s\" already exists as an active slot\n"),
							  slot_name);
		return SLOT_ACTIVE;
	}

	return SLOT_NOT_FOUND;
}


bool
create_replication_slot_replprot(UXconn *conn, UXconn *repl_conn, char *slot_name, UXSQLExpBufferData *error_msg)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	ReplSlotStatus slot_status = _verify_replication_slot(conn, slot_name, error_msg);

	/* Replication slot is unusable */
	if (slot_status == SLOT_NOT_PHYSICAL || slot_status == SLOT_ACTIVE)
		return false;

	/* Replication slot can be reused */
	if (slot_status == SLOT_INACTIVE)
		return true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "CREATE_REPLICATION_SLOT %s PHYSICAL",
					  slot_name);

	/* In 9.6 and later, reserve the LSN straight away */
	if (UXSQLserverVersion(conn) >= 90600)
	{
		appendUXSQLExpBufferStr(&query,
							 " RESERVE_WAL");
	}

	appendUXSQLExpBufferChar(&query, ';');

	res = UXSQLexec(repl_conn, query.data);


	if ((UXSQLresultStatus(res) != UXRES_TUPLES_OK || !UXSQLntuples(res)) && error_msg != NULL)
	{
		appendUXSQLExpBuffer(error_msg,
						  _("unable to create replication slot \"%s\" on the upstream node: %s\n"),
						  slot_name,
							 UXSQLerrorMessage(conn));
		success = false;
	}

	UXSQLclear(res);
	return success;
}


bool
create_replication_slot_sql(UXconn *conn, char *slot_name, UXSQLExpBufferData *error_msg)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	ReplSlotStatus slot_status = _verify_replication_slot(conn, slot_name, error_msg);

	/* Replication slot is unusable */
	if (slot_status == SLOT_NOT_PHYSICAL || slot_status == SLOT_ACTIVE)
		return false;

	/* Replication slot can be reused */
	if (slot_status == SLOT_INACTIVE)
		return true;

	initUXSQLExpBuffer(&query);

	/* In 9.6 and later, reserve the LSN straight away */
	if (UXSQLserverVersion(conn) >= 90600)
	{
		appendUXSQLExpBuffer(&query,
						  "SELECT * FROM ux_catalog.ux_create_physical_replication_slot('%s', TRUE)",
						  slot_name);
	}
	else
	{
		appendUXSQLExpBuffer(&query,
						  "SELECT * FROM ux_catalog.ux_create_physical_replication_slot('%s')",
						  slot_name);
	}

	log_debug(_("create_replication_slot_sql(): creating slot \"%s\" on upstream"), slot_name);
	log_verbose(LOG_DEBUG, "create_replication_slot_sql():\n%s", query.data);

	res = UXSQLexec(conn, query.data);
	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK && error_msg != NULL)
	{
		appendUXSQLExpBuffer(error_msg,
						  _("unable to create replication slot \"%s\" on the upstream node: %s\n"),
						  slot_name,
						  UXSQLerrorMessage(conn));
		success = false;
	}

	UXSQLclear(res);
	return success;
}


bool
drop_replication_slot_sql(UXconn *conn, char *slot_name)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT ux_catalog.ux_drop_replication_slot('%s')",
					  slot_name);

	log_verbose(LOG_DEBUG, "drop_replication_slot_sql():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("drop_replication_slot_sql(): unable to drop replication slot \"%s\""),
					 slot_name);

		success = false;
	}
	else
	{
		log_verbose(LOG_DEBUG, "replication slot \"%s\" successfully dropped",
					slot_name);
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


bool
drop_replication_slot_replprot(UXconn *repl_conn, char *slot_name)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "DROP_REPLICATION_SLOT %s",
					  slot_name);

	log_verbose(LOG_DEBUG, "drop_replication_slot_replprot():\n  %s", query.data);

	res = UXSQLexec(repl_conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(repl_conn, query.data,
					 _("drop_replication_slot_sql(): unable to drop replication slot \"%s\""),
					 slot_name);

		success = false;
	}
	else
	{
		log_verbose(LOG_DEBUG, "replication slot \"%s\" successfully dropped",
					slot_name);
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


RecordStatus
get_slot_record(UXconn *conn, char *slot_name, t_replication_slot *record)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	RecordStatus record_status = RECORD_FOUND;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT slot_name, slot_type, active "
					  "  FROM ux_catalog.ux_replication_slots "
					  " WHERE slot_name = '%s' ",
					  slot_name);

	log_verbose(LOG_DEBUG, "get_slot_record():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_slot_record(): unable to query ux_replication_slots"));

		record_status = RECORD_ERROR;
	}
	else if (!UXSQLntuples(res))
	{
		record_status = RECORD_NOT_FOUND;
	}
	else
	{
		snprintf(record->slot_name,
				 sizeof(record->slot_name),
				 "%s", UXSQLgetvalue(res, 0, 0));
		snprintf(record->slot_type,
				 sizeof(record->slot_type),
				 "%s", UXSQLgetvalue(res, 0, 1));
		record->active = atobool(UXSQLgetvalue(res, 0, 2));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return record_status;
}


int
get_free_replication_slot_count(UXconn *conn,  int *max_replication_slots)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			free_slots = 0;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 " SELECT ux_catalog.current_setting('max_replication_slots')::INT - "
						 "          ux_catalog.count(*) "
						 "          AS free_slots, "
						 "        ux_catalog.current_setting('max_replication_slots')::INT "
						 "          AS max_replication_slots "
						 "   FROM ux_catalog.ux_replication_slots s"
						 "  WHERE s.slot_type = 'physical'");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_free_replication_slot_count(): unable to execute replication slot query"));

		free_slots = UNKNOWN_VALUE;
	}
	else if (UXSQLntuples(res) == 0)
	{
		free_slots = UNKNOWN_VALUE;
	}
	else
	{
		free_slots = atoi(UXSQLgetvalue(res, 0, 0));
		if (max_replication_slots != NULL)
			*max_replication_slots = atoi(UXSQLgetvalue(res, 0, 1));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return free_slots;
}


int
get_inactive_replication_slots(UXconn *conn, KeyValueList *list)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			i, inactive_slots = 0;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "   SELECT slot_name, slot_type "
						 "     FROM ux_catalog.ux_replication_slots "
						 "    WHERE active IS FALSE "
						 "      AND slot_type = 'physical' "
						 " ORDER BY slot_name ");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_inactive_replication_slots(): unable to execute replication slot query"));

		inactive_slots = -1;
	}
	else
	{
		inactive_slots = UXSQLntuples(res);

		for (i = 0; i < inactive_slots; i++)
		{
			key_value_list_set(list,
							   UXSQLgetvalue(res, i, 0),
							   UXSQLgetvalue(res, i, 1));
		}
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return inactive_slots;
}



/* ==================== */
/* tablespace functions */
/* ==================== */

bool
get_tablespace_name_by_location(UXconn *conn, const char *location, char *name)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool	    success = true;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT spcname "
					  "  FROM ux_catalog.ux_tablespace "
					  " WHERE ux_catalog.ux_tablespace_location(oid) = '%s'",
					  location);

	log_verbose(LOG_DEBUG, "get_tablespace_name_by_location():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data,
					 _("get_tablespace_name_by_location(): unable to execute tablespace query"));
		success = false;
	}
	else if (UXSQLntuples(res) == 0)
	{
		success = false;
	}
	else
	{
		snprintf(name, MAXLEN,
				 "%s", UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}

/* ============================ */
/* asynchronous query functions */
/* ============================ */

bool
cancel_query(UXconn *conn, int timeout)
{
	char		errbuf[ERRBUFF_SIZE] = "";
	UXcancel   *uxcancel = NULL;

	if (wait_connection_availability(conn, timeout) != 1)
		return false;

	uxcancel = UXSQLgetCancel(conn);

	if (uxcancel == NULL)
		return false;

	/*
	 * UXSQLcancel can only return 0 if socket()/connect()/send() fails, in any
	 * of those cases we can assume something bad happened to the connection
	 */
	if (UXSQLcancel(uxcancel, errbuf, ERRBUFF_SIZE) == 0)
	{
		log_warning(_("unable to cancel current query"));
		log_detail("\n%s", errbuf);
		UXSQLfreeCancel(uxcancel);
		return false;
	}

	UXSQLfreeCancel(uxcancel);

	return true;
}


/*
 * Wait until current query finishes, ignoring any results.
 * Usually this will be an async query or query cancellation.
 *
 * Returns 1 for success; 0 if any error occurred; -1 if timeout reached.
 */
int
wait_connection_availability(UXconn *conn, int timeout)
{
	UXresult   *res = NULL;
	fd_set		read_set;
	int			sock = UXSQLsocket(conn);
	struct timeval tmout,
				before,
				after;
	struct timezone tz;
	long long	timeout_ms;

	/* calculate timeout in microseconds */
	timeout_ms = (long long) timeout * 1000000;

	while (timeout_ms > 0)
	{
		if (UXSQLconsumeInput(conn) == 0)
		{
			log_warning(_("wait_connection_availability(): unable to receive data from connection"));
			log_detail("%s", UXSQLerrorMessage(conn));
			return 0;
		}

		if (UXSQLisBusy(conn) == 0)
		{
			do
			{
				res = UXSQLgetResult(conn);
				UXSQLclear(res);
			} while (res != NULL);

			break;
		}

		tmout.tv_sec = 0;
		tmout.tv_usec = 250000;

		FD_ZERO(&read_set);
		FD_SET(sock, &read_set);

		gettimeofday(&before, &tz);
		if (select(sock, &read_set, NULL, NULL, &tmout) == -1)
		{
			log_warning(_("wait_connection_availability(): select() returned with error"));
			log_detail("%s", strerror(errno));
			return -1;
		}

		gettimeofday(&after, &tz);

		timeout_ms -= (after.tv_sec * 1000000 + after.tv_usec) -
			(before.tv_sec * 1000000 + before.tv_usec);
	}


	if (timeout_ms >= 0)
	{
		return 1;
	}

	log_warning(_("wait_connection_availability(): timeout (%i secs) reached"), timeout);
	return -1;
}


/* =========================== */
/* node availability functions */
/* =========================== */

bool
is_server_available(const char *conninfo)
{
	return _is_server_available(conninfo, false);
}


bool
is_server_available_quiet(const char *conninfo)
{
	return _is_server_available(conninfo, true);
}


static bool
_is_server_available(const char *conninfo, bool quiet)
{
	UXPing		status = UXSQLping(conninfo);

	log_verbose(LOG_DEBUG, "is_server_available(): ping status for \"%s\" is %s", conninfo, print_uxsqlping_status(status));
	if (status == UXSQLPING_OK)
		return true;

	if (quiet == false)
	{
		log_warning(_("unable to ping \"%s\""), conninfo);
		log_detail(_("UXSQLping() returned \"%s\""), print_uxsqlping_status(status));
	}

	return false;
}


bool
is_server_available_params(t_conninfo_param_list *param_list)
{
	UXPing		status = UXSQLpingParams((const char **) param_list->keywords,
									  (const char **) param_list->values,
									  false);

	/* deparsing the param_list adds overhead, so only do it if needed  */
	if (log_level == LOG_DEBUG || status != UXSQLPING_OK)
	{
		char *conninfo_str = param_list_to_string(param_list);
		log_verbose(LOG_DEBUG, "is_server_available_params(): ping status for \"%s\" is %s", conninfo_str, print_uxsqlping_status(status));

		if (status != UXSQLPING_OK)
		{
			log_warning(_("unable to ping \"%s\""), conninfo_str);
			log_detail(_("UXSQLping() returned \"%s\""), print_uxsqlping_status(status));
		}

		pfree(conninfo_str);
	}

	if (status == UXSQLPING_OK)
		return true;

	return false;
}



/*
 * Simple throw-away query to stop a connection handle going stale.
 */
ExecStatusType
connection_ping(UXconn *conn)
{
	UXresult   *res = UXSQLexec(conn, "SELECT TRUE");
	ExecStatusType ping_result;

	log_verbose(LOG_DEBUG, "connection_ping(): result is %s", UXSQLresStatus(UXSQLresultStatus(res)));

	ping_result = UXSQLresultStatus(res);
	UXSQLclear(res);

	return ping_result;
}


ExecStatusType
connection_ping_reconnect(UXconn *conn)
{
	ExecStatusType ping_result = connection_ping(conn);

	if (UXSQLstatus(conn) != CONNECTION_OK)
	{
		log_warning(_("connection error, attempting to reset"));
		log_detail("\n%s", UXSQLerrorMessage(conn));
		UXSQLreset(conn);
		ping_result = connection_ping(conn);
	}

	log_verbose(LOG_DEBUG, "connection_ping_reconnect(): result is %s", UXSQLresStatus(ping_result));

	return ping_result;
}



/* ==================== */
/* monitoring functions */
/* ==================== */

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
)
{
	UXSQLExpBufferData query;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "INSERT INTO repmgr.monitoring_history "
					  "           (primary_node_id, "
					  "            standby_node_id, "
					  "            last_monitor_time, "
					  "            last_apply_time, "
					  "            last_wal_primary_location, "
					  "            last_wal_standby_location, "
					  "            replication_lag, "
					  "            apply_lag ) "
					  "     VALUES(%i, "
					  "            %i, "
					  "            '%s'::TIMESTAMP WITH TIME ZONE, "
					  "            '%s'::TIMESTAMP WITH TIME ZONE, "
					  "            '%X/%X', "
					  "            '%X/%X', "
					  "            %llu, "
					  "            %llu) ",
					  primary_node_id,
					  local_node_id,
					  monitor_standby_timestamp,
					  last_xact_replay_timestamp,
					  format_lsn(primary_last_wal_location),
					  format_lsn(last_wal_receive_lsn),
					  replication_lag_bytes,
					  apply_lag_bytes);

	log_verbose(LOG_DEBUG, "standby_monitor:()\n%s", query.data);

	if (UXSQLsendQuery(primary_conn, query.data) == 0)
	{
		log_warning(_("query could not be sent to primary:\n  %s"),
					UXSQLerrorMessage(primary_conn));
	}
	else
	{
		UXresult   *res = UXSQLexec(local_conn, "SELECT repmgr.standby_set_last_updated()");

		/* not critical if the above query fails */
		if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
			log_warning(_("add_monitoring_record(): unable to set last_updated:\n  %s"),
						UXSQLerrorMessage(local_conn));

		UXSQLclear(res);
	}

	termUXSQLExpBuffer(&query);

	return;
}


int
get_number_of_monitoring_records_to_delete(UXconn *primary_conn, int keep_history, int node_id)
{
	UXSQLExpBufferData query;
	int				record_count = -1;
	UXresult	   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT ux_catalog.count(*) "
					  "  FROM repmgr.monitoring_history "
					  " WHERE ux_catalog.age(ux_catalog.now(), last_monitor_time) >= '%d days'::interval",
					  keep_history);

	if (node_id != UNKNOWN_NODE_ID)
	{
		appendUXSQLExpBuffer(&query,
						  "  AND standby_node_id = %i", node_id);
	}

	log_verbose(LOG_DEBUG, "get_number_of_monitoring_records_to_delete():\n  %s", query.data);

	res = UXSQLexec(primary_conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(primary_conn, query.data,
					 _("get_number_of_monitoring_records_to_delete(): unable to query number of monitoring records to clean up"));
	}
	else
	{
		record_count = atoi(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return record_count;
}


bool
delete_monitoring_records(UXconn *primary_conn, int keep_history, int node_id)
{
	UXSQLExpBufferData query;
	bool			success = true;
	UXresult	   *res = NULL;

	initUXSQLExpBuffer(&query);

	if (keep_history > 0 || node_id != UNKNOWN_NODE_ID)
	{
		appendUXSQLExpBuffer(&query,
						  "DELETE FROM repmgr.monitoring_history "
						  " WHERE ux_catalog.age(ux_catalog.now(), last_monitor_time) >= '%d days'::INTERVAL ",
						  keep_history);

		if (node_id != UNKNOWN_NODE_ID)
		{
			appendUXSQLExpBuffer(&query,
							  "  AND standby_node_id = %i", node_id);
		}
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 "TRUNCATE TABLE repmgr.monitoring_history");
	}

	res = UXSQLexec(primary_conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(primary_conn, query.data,
					 _("delete_monitoring_records(): unable to delete monitoring records"));
		success = false;
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}

/*
 * node voting functions
 *
 * These are intended to run under repmgrd and mainly rely on shared memory
 */

int
get_current_term(UXconn *conn)
{
	UXresult   *res = NULL;
	int term = VOTING_TERM_NOT_SET;

	res = UXSQLexec(conn, "SELECT term FROM repmgr.voting_term");

	/* it doesn't matter if for whatever reason the table has no rows */

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, NULL,
					 _("get_current_term(): unable to query \"repmgr.voting_term\""));
	}
	else if (UXSQLntuples(res) > 0)
	{
		term = atoi(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);
	return term;
}


void
initialize_voting_term(UXconn *conn)
{
	UXresult   *res = NULL;

	int current_term = get_current_term(conn);

	if (current_term == VOTING_TERM_NOT_SET)
	{
		res = UXSQLexec(conn, "INSERT INTO repmgr.voting_term (term) VALUES (1)");
	}
	else
	{
		res = UXSQLexec(conn, "UPDATE repmgr.voting_term SET term = 1");
	}

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("unable to initialize repmgr.voting_term"));
	}

	UXSQLclear(res);
	return;
}


void
increment_current_term(UXconn *conn)
{
	UXresult   *res = NULL;

	res = UXSQLexec(conn, "UPDATE repmgr.voting_term SET term = term + 1");

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, NULL, _("unable to increment repmgr.voting_term"));
	}

	UXSQLclear(res);
	return;
}


bool
announce_candidature(UXconn *conn, t_node_info *this_node, t_node_info *other_node, int electoral_term)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	bool		retval = false;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT repmgr.other_node_is_candidate(%i, %i)",
					  this_node->node_id,
					  electoral_term);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("announce_candidature(): unable to execute repmgr.other_node_is_candidate()"));
	}
	else
	{
		retval = atobool(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return retval;
}


void
notify_follow_primary(UXconn *conn, int primary_node_id)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  "SELECT repmgr.notify_follow_primary(%i)",
					  primary_node_id);

	log_verbose(LOG_DEBUG, "notify_follow_primary():\n  %s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute repmgr.notify_follow_primary()"));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return;
}


bool
get_new_primary(UXconn *conn, int *primary_node_id)
{
	UXresult   *res = NULL;
	int			new_primary_node_id = UNKNOWN_NODE_ID;
	bool		success = true;

	const char *sqlquery = "SELECT repmgr.get_new_primary()";

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to execute repmgr.get_new_primary()"));
		success = false;
	}
	else if (UXSQLgetisnull(res, 0, 0))
	{
		success = false;
	}
	else
	{
		new_primary_node_id = atoi(UXSQLgetvalue(res, 0, 0));
	}

	UXSQLclear(res);

	/*
	 * repmgr.get_new_primary() will return UNKNOWN_NODE_ID if
	 * "follow_new_primary" is false
	 */
	if (new_primary_node_id == UNKNOWN_NODE_ID)
		success = false;

	*primary_node_id = new_primary_node_id;

	return success;
}


void
reset_voting_status(UXconn *conn)
{
	UXresult   *res = NULL;

	const char *sqlquery = "SELECT repmgr.reset_voting_status()";

	res = UXSQLexec(conn, sqlquery);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, sqlquery, _("unable to execute repmgr.reset_voting_status()"));
	}

	UXSQLclear(res);
	return;
}

/*BEGIN: Modified by douwen for bug #179168, 2023/3/16,reviewer:huyn*/
/*
 * check repliction mode 
 * sync OR async 
 */
char *
check_repliction_sync_async(UXconn *conn)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	char       *resvalue = NULL;
	char       *repliction_state = NULL;
	int        len = 0;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query, "SELECT sync_state from ux_stat_replication;");
	res = UXSQLexec(conn, query.data);
	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to execute query\n"));
		return NULL;
	}
	
	resvalue = UXSQLgetvalue(res, 0, 0);
	
	if (resvalue != NULL)
	{
		len = strlen(resvalue)+1;
		repliction_state = ux_malloc0(len);
		strncpy(repliction_state,resvalue,len);
	}
	UXSQLclear(res);
	return repliction_state;
}
/*END: Modified by douwen for bug #179168, 2023/3/16,reviewer:huyn*/

/* ============================ */
/* replication status functions */
/* ============================ */

/*
 * Returns the current LSN on the primary.
 *
 * This just executes "ux_current_wal_lsn()".
 *
 * Function "get_node_current_lsn()" below will return the latest
 * LSN regardless of recovery state.
 */
XLogRecPtr
get_primary_current_lsn(UXconn *conn)
{
	UXresult   *res = NULL;
	XLogRecPtr	ptr = InvalidXLogRecPtr;

	if (UXSQLserverVersion(conn) >= 100000)
	{
		res = UXSQLexec(conn, "SELECT ux_catalog.ux_current_wal_lsn()");
	}
	else
	{
		res = UXSQLexec(conn, "SELECT ux_catalog.ux_current_xlog_location()");
	}

	if (UXSQLresultStatus(res) == UXRES_TUPLES_OK)
	{
		ptr = parse_lsn(UXSQLgetvalue(res, 0, 0));
	}
	else
	{
		log_db_error(conn, NULL, _("unable to execute get_primary_current_lsn()"));
	}


	UXSQLclear(res);

	return ptr;
}


XLogRecPtr
get_last_wal_receive_location(UXconn *conn)
{
	UXresult   *res = NULL;
	XLogRecPtr	ptr = InvalidXLogRecPtr;

	if (UXSQLserverVersion(conn) >= 100000)
	{
		res = UXSQLexec(conn, "SELECT ux_catalog.ux_last_wal_receive_lsn()");
	}
	else
	{
		res = UXSQLexec(conn, "SELECT ux_catalog.ux_last_xlog_receive_location()");
	}

	if (UXSQLresultStatus(res) == UXRES_TUPLES_OK)
	{
		ptr = parse_lsn(UXSQLgetvalue(res, 0, 0));
	}
	else
	{
		log_db_error(conn, NULL, _("unable to execute get_last_wal_receive_location()"));
	}

	UXSQLclear(res);

	return ptr;
}

/*
 * Returns the latest LSN for the node regardless of recovery state.
 */
XLogRecPtr
get_node_current_lsn(UXconn *conn)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	XLogRecPtr	ptr = InvalidXLogRecPtr;

	initUXSQLExpBuffer(&query);

	if (UXSQLserverVersion(conn) >= 100000)
	{
		appendUXSQLExpBufferStr(&query,
							 " WITH lsn_states AS ( "
							 "  SELECT "
							 "    CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "      THEN ux_catalog.ux_current_wal_lsn() "
							 "      ELSE NULL "
							 "    END "
							 "      AS current_wal_lsn, "
							 "    CASE WHEN ux_catalog.ux_is_in_recovery() IS TRUE "
							 "      THEN ux_catalog.ux_last_wal_receive_lsn() "
							 "      ELSE NULL "
							 "    END "
							 "      AS last_wal_receive_lsn, "
							 "    CASE WHEN ux_catalog.ux_is_in_recovery() IS TRUE "
							 "      THEN ux_catalog.ux_last_wal_replay_lsn() "
							 "      ELSE NULL "
							 "     END "
							 "       AS last_wal_replay_lsn "
							 " ) ");
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 " WITH lsn_states AS ( "
							 "  SELECT "
							 "    CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "      THEN ux_catalog.ux_current_xlog_location() "
							 "      ELSE NULL "
							 "    END "
							 "      AS current_wal_lsn, "
							 "    CASE WHEN ux_catalog.ux_is_in_recovery() IS TRUE "
							 "      THEN ux_catalog.ux_last_xlog_receive_location() "
							 "      ELSE NULL "
							 "    END "
							 "      AS last_wal_receive_lsn, "
							 "    CASE WHEN ux_catalog.ux_is_in_recovery() IS TRUE "
							 "      THEN ux_catalog.ux_last_xlog_replay_location() "
							 "      ELSE NULL "
							 "     END "
							 "       AS last_wal_replay_lsn "
							 " ) ");
	}

	appendUXSQLExpBufferStr(&query,
						 " SELECT "
						 "   CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
						 "     THEN current_wal_lsn "
						 "     ELSE "
						 "       CASE WHEN last_wal_receive_lsn IS NULL "
						 "       THEN last_wal_replay_lsn "
						 "         ELSE "
						 "           CASE WHEN last_wal_replay_lsn > last_wal_receive_lsn "
						 "             THEN last_wal_replay_lsn "
						 "             ELSE last_wal_receive_lsn "
						 "           END "
						 "       END "
						 "   END "
						 "     AS current_lsn "
						 "   FROM lsn_states ");

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute get_node_current_lsn()"));
	}
	else if (!UXSQLgetisnull(res, 0, 0))
	{
		ptr = parse_lsn(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return ptr;
}


void
init_replication_info(ReplInfo *replication_info)
{
	memset(replication_info->current_timestamp, 0, sizeof(replication_info->current_timestamp));
	replication_info->in_recovery = false;
	replication_info->timeline_id = UNKNOWN_TIMELINE_ID;
	replication_info->last_wal_receive_lsn = InvalidXLogRecPtr;
	replication_info->last_wal_replay_lsn = InvalidXLogRecPtr;
	memset(replication_info->last_xact_replay_timestamp, 0, sizeof(replication_info->last_xact_replay_timestamp));
	replication_info->replication_lag_time = 0;
	replication_info->receiving_streamed_wal = true;
	replication_info->wal_replay_paused = false;
	replication_info->upstream_last_seen = -1;
	replication_info->upstream_node_id = UNKNOWN_NODE_ID;
}


bool
get_replication_info(UXconn *conn, t_server_type node_type, ReplInfo *replication_info)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		success = true;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBufferStr(&query,
						 " SELECT ts, "
						 "        in_recovery, "
						 "        last_wal_receive_lsn, "
						 "        last_wal_replay_lsn, "
						 "        last_xact_replay_timestamp, "
						 "        CASE WHEN (last_wal_receive_lsn = last_wal_replay_lsn) "
						 "          THEN 0::INT "
						 "        ELSE "
						 "          CASE WHEN last_xact_replay_timestamp IS NULL "
						 "            THEN 0::INT "
						 "          ELSE "
						 "            EXTRACT(epoch FROM (ux_catalog.clock_timestamp() - last_xact_replay_timestamp))::INT "
						 "          END "
						 "        END AS replication_lag_time, "
						 "        last_wal_receive_lsn >= last_wal_replay_lsn AS receiving_streamed_wal, "
						 "        wal_replay_paused, "
						 "        upstream_last_seen, "
						 "        upstream_node_id "
						 "   FROM ( "
						 " SELECT CURRENT_TIMESTAMP AS ts, "
						 "        ux_catalog.ux_is_in_recovery() AS in_recovery, "
						 "        ux_catalog.ux_last_xact_replay_timestamp() AS last_xact_replay_timestamp, ");


	if (UXSQLserverVersion(conn) >= 100000)
	{
		appendUXSQLExpBufferStr(&query,
							 "        COALESCE(ux_catalog.ux_last_wal_receive_lsn(), '0/0'::UX_LSN) AS last_wal_receive_lsn, "
							 "        COALESCE(ux_catalog.ux_last_wal_replay_lsn(),  '0/0'::UX_LSN) AS last_wal_replay_lsn, "
							 "        CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "          THEN FALSE "
							 "          ELSE ux_catalog.ux_is_wal_replay_paused() "
							 "        END AS wal_replay_paused, ");
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 "        COALESCE(ux_catalog.ux_last_xlog_receive_location(), '0/0'::UX_LSN) AS last_wal_receive_lsn, "
							 "        COALESCE(ux_catalog.ux_last_xlog_replay_location(),  '0/0'::UX_LSN) AS last_wal_replay_lsn, "
							 "        CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "          THEN FALSE "
							 "          ELSE ux_catalog.ux_is_xlog_replay_paused() "
							 "        END AS wal_replay_paused, ");
	}

	/* Add information about upstream node from shared memory */
	if (node_type == WITNESS)
	{
		appendUXSQLExpBufferStr(&query,
							 "        repmgr.get_upstream_last_seen() AS upstream_last_seen, "
							 "        repmgr.get_upstream_node_id() AS upstream_node_id ");
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 "        CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "          THEN -1 "
							 "          ELSE repmgr.get_upstream_last_seen() "
							 "        END AS upstream_last_seen, ");
		appendUXSQLExpBufferStr(&query,
							 "        CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "          THEN -1 "
							 "          ELSE repmgr.get_upstream_node_id() "
							 "        END AS upstream_node_id ");
	}

	appendUXSQLExpBufferStr(&query,
						 "          ) q ");

	log_verbose(LOG_DEBUG, "get_replication_info():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK || !UXSQLntuples(res))
	{
		log_db_error(conn, query.data, _("get_replication_info(): unable to execute query"));

		success = false;
	}
	else
	{
		snprintf(replication_info->current_timestamp,
				 sizeof(replication_info->current_timestamp),
				 "%s", UXSQLgetvalue(res, 0, 0));
		replication_info->in_recovery = atobool(UXSQLgetvalue(res, 0, 1));
		replication_info->last_wal_receive_lsn = parse_lsn(UXSQLgetvalue(res, 0, 2));
		replication_info->last_wal_replay_lsn = parse_lsn(UXSQLgetvalue(res, 0, 3));
		snprintf(replication_info->last_xact_replay_timestamp,
				 sizeof(replication_info->last_xact_replay_timestamp),
				 "%s", UXSQLgetvalue(res, 0, 4));
		replication_info->replication_lag_time = atoi(UXSQLgetvalue(res, 0, 5));
		replication_info->receiving_streamed_wal = atobool(UXSQLgetvalue(res, 0, 6));
		replication_info->wal_replay_paused = atobool(UXSQLgetvalue(res, 0, 7));
		replication_info->upstream_last_seen = atoi(UXSQLgetvalue(res, 0, 8));
		replication_info->upstream_node_id = atoi(UXSQLgetvalue(res, 0, 9));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return success;
}


int
get_replication_lag_seconds(UXconn *conn)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int			lag_seconds = 0;

	initUXSQLExpBuffer(&query);

	if (UXSQLserverVersion(conn) >= 100000)
	{
		appendUXSQLExpBufferStr(&query,
							 " SELECT CASE WHEN (ux_catalog.ux_last_wal_receive_lsn() = ux_catalog.ux_last_wal_replay_lsn()) ");

	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 " SELECT CASE WHEN (ux_catalog.ux_last_xlog_receive_location() = ux_catalog.ux_last_xlog_replay_location()) ");
	}

	appendUXSQLExpBufferStr(&query,
						 "          THEN 0 ");
	
	/* Modify by houjiaxing for #176963 at 2023/2/15 , reviewer:huyuanni */
	appendUXSQLExpBufferStr(&query,
						 "        ELSE EXTRACT(epoch FROM (ux_catalog.clock_timestamp() - ux_catalog.ux_last_xact_replay_timestamp()))::INT ");

	appendUXSQLExpBufferStr(&query,
						 "          END "
						 "        AS lag_seconds");

	res = UXSQLexec(conn, query.data);
	log_verbose(LOG_DEBUG, "get_replication_lag_seconds():\n%s", query.data);
	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_warning("%s", UXSQLerrorMessage(conn));
		UXSQLclear(res);

		return UNKNOWN_REPLICATION_LAG;
	}

	if (!UXSQLntuples(res))
	{
		return UNKNOWN_REPLICATION_LAG;
	}

	lag_seconds = atoi(UXSQLgetvalue(res, 0, 0));

	UXSQLclear(res);
	return lag_seconds;
}



TimeLineID
get_node_timeline(UXconn *conn, char *timeline_id_str)
{
	TimeLineID timeline_id  = UNKNOWN_TIMELINE_ID;

	/*
	 * ux_control_checkpoint() was introduced in UxsinoDB 9.6
	 */
	if (UXSQLserverVersion(conn) >= 90600)
	{
		UXresult   *res = NULL;

		res = UXSQLexec(conn, "SELECT timeline_id FROM ux_catalog.ux_control_checkpoint()");

		if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
		{
			log_db_error(conn, NULL, _("get_node_timeline(): unable to query ux_control_system()"));
		}
		else
		{
			timeline_id = atoi(UXSQLgetvalue(res, 0, 0));
		}

		UXSQLclear(res);
	}

	/* If requested, format the timeline ID as a string */
	if (timeline_id_str != NULL)
	{
		if (timeline_id == UNKNOWN_TIMELINE_ID)
		{
			strncpy(timeline_id_str, "?", MAXLEN);
		}
		else
		{
			snprintf(timeline_id_str, MAXLEN, "%i", timeline_id);
		}
	}

	return timeline_id;
}


void
get_node_replication_stats(UXconn *conn, t_node_info *node_info)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 " SELECT ux_catalog.current_setting('max_wal_senders')::INT AS max_wal_senders, "
						 "        (SELECT ux_catalog.count(*) FROM ux_catalog.ux_stat_replication) AS attached_wal_receivers, "
						 "        current_setting('max_replication_slots')::INT AS max_replication_slots, "
						 "        (SELECT ux_catalog.count(*) FROM ux_catalog.ux_replication_slots WHERE slot_type='physical') AS total_replication_slots, "
						 "        (SELECT ux_catalog.count(*) FROM ux_catalog.ux_replication_slots WHERE active IS TRUE AND slot_type='physical')  AS active_replication_slots, "
						 "        (SELECT ux_catalog.count(*) FROM ux_catalog.ux_replication_slots WHERE active IS FALSE AND slot_type='physical') AS inactive_replication_slots, "						 
						 "        ux_catalog.ux_is_in_recovery() AS in_recovery");

	log_verbose(LOG_DEBUG, "get_node_replication_stats():\n%s", query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_warning(_("unable to retrieve node replication statistics"));
		log_detail("%s", UXSQLerrorMessage(conn));
		log_detail("%s", query.data);

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		return;
	}

	node_info->max_wal_senders = atoi(UXSQLgetvalue(res, 0, 0));
	node_info->attached_wal_receivers = atoi(UXSQLgetvalue(res, 0, 1));
	node_info->max_replication_slots = atoi(UXSQLgetvalue(res, 0, 2));
	node_info->total_replication_slots = atoi(UXSQLgetvalue(res, 0, 3));
	node_info->active_replication_slots = atoi(UXSQLgetvalue(res, 0, 4));
	node_info->inactive_replication_slots = atoi(UXSQLgetvalue(res, 0, 5));
	node_info->recovery_type = strcmp(UXSQLgetvalue(res, 0, 6), "f") == 0 ? RECTYPE_PRIMARY : RECTYPE_STANDBY;

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return;
}

NodeAttached
is_downstream_node_attached(UXconn *conn, char *node_name, char **node_state)
{
	return _is_downstream_node_attached(conn, node_name, node_state, false);
}

NodeAttached
is_downstream_node_attached_quiet(UXconn *conn, char *node_name, char **node_state)
{
	return _is_downstream_node_attached(conn, node_name, node_state, true);
}

NodeAttached
_is_downstream_node_attached(UXconn *conn, char *node_name, char **node_state, bool quiet)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
					  " SELECT pid, state "
					  "   FROM ux_catalog.ux_stat_replication "
					  "  WHERE application_name = '%s'",
					  node_name);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_verbose(LOG_WARNING, _("unable to query ux_stat_replication"));
		log_detail("%s", UXSQLerrorMessage(conn));
		log_detail("%s", query.data);

		termUXSQLExpBuffer(&query);
		UXSQLclear(res);

		return NODE_ATTACHED_UNKNOWN;
	}

	termUXSQLExpBuffer(&query);

	/*
	 * If there's more than one entry in ux_stat_application, there's no
	 * way we can reliably determine which one belongs to the node we're
	 * checking, so there's nothing more we can do.
	 */
	if (UXSQLntuples(res) > 1)
	{
		if (quiet == false)
		{
			log_error(_("multiple entries with \"application_name\" set to  \"%s\" found in \"ux_stat_replication\""),
					  node_name);
			log_hint(_("verify that a unique node name is configured for each node"));
		}

		UXSQLclear(res);

		return NODE_ATTACHED_UNKNOWN;
	}

	if (UXSQLntuples(res) == 0)
	{
		if (quiet == false)
			log_warning(_("node \"%s\" not found in \"ux_stat_replication\""), node_name);

		UXSQLclear(res);

		return NODE_DETACHED;
	}

	/*
	 * If the connection is not a superuser or member of pg_read_all_stats, we
	 * won't be able to retrieve the "state" column, so we'll assume
	 * the node is attached.
	 */

	if (connection_has_ux_monitor_role(conn, "ux_read_all_stats"))
	{
		const char *state = UXSQLgetvalue(res, 0, 1);

		if (node_state != NULL)
		{
			int		state_len = strlen(state);
			*node_state = palloc0(state_len + 1);
			strncpy(*node_state, state, state_len);
		}

		if (strcmp(state, "streaming") != 0)
		{
			if (quiet == false)
				log_warning(_("node \"%s\" attached in state \"%s\""),
							node_name,
							state);

			UXSQLclear(res);

			return NODE_NOT_ATTACHED;
		}
	}
	else if (node_state != NULL)
	{
		*node_state = palloc0(1);
		*node_state[0] = '\0';
	}

	UXSQLclear(res);

	return NODE_ATTACHED;
}


void
set_upstream_last_seen(UXconn *conn, int upstream_node_id)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query,
						 "SELECT repmgr.set_upstream_last_seen(%i)",
						 upstream_node_id);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute repmgr.set_upstream_last_seen()"));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);
}


int
get_upstream_last_seen(UXconn *conn, t_server_type node_type)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	int upstream_last_seen = -1;

	initUXSQLExpBuffer(&query);

	if (node_type == WITNESS)
	{
		appendUXSQLExpBufferStr(&query,
							 "SELECT repmgr.get_upstream_last_seen()");
	}
	else
	{
		appendUXSQLExpBufferStr(&query,
							 "SELECT CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "   THEN -1 "
							 "   ELSE repmgr.get_upstream_last_seen() "
							 " END AS upstream_last_seen ");
	}

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute repmgr.get_upstream_last_seen()"));
	}
	else
	{
		upstream_last_seen = atoi(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return upstream_last_seen;
}


bool
is_wal_replay_paused(UXconn *conn, bool check_pending_wal)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	bool		is_paused = false;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 "SELECT paused.wal_replay_paused ");

	if (UXSQLserverVersion(conn) >= 100000)
	{
		if (check_pending_wal == true)
		{
			appendUXSQLExpBufferStr(&query,
								 " AND ux_catalog.ux_last_wal_replay_lsn() < ux_catalog.ux_last_wal_receive_lsn() ");
		}

		appendUXSQLExpBufferStr(&query,
							 " FROM (SELECT CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "                THEN FALSE "
							 "                ELSE ux_catalog.ux_is_wal_replay_paused() "
							 "              END AS wal_replay_paused) paused ");
	}
	else
	{
		if (check_pending_wal == true)
		{
			appendUXSQLExpBufferStr(&query,
								 " AND ux_catalog.ux_last_xlog_replay_location() < ux_catalog.ux_last_xlog_receive_location() ");
		}

		appendUXSQLExpBufferStr(&query,
							 " FROM (SELECT CASE WHEN ux_catalog.ux_is_in_recovery() IS FALSE "
							 "                THEN FALSE "
							 "                ELSE ux_catalog.ux_is_xlog_replay_paused() "
							 "              END AS wal_replay_paused) paused ");

	}

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute WAL replay pause query"));
	}
	else
	{
		is_paused = atobool(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);

	return is_paused;
}

/* repmgrd status functions */

CheckStatus
get_repmgrd_status(UXconn *conn)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;
	CheckStatus	repmgrd_status = CHECK_STATUS_CRITICAL;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBufferStr(&query,
						 " SELECT "
						 " CASE "
						 "   WHEN repmgr.repmgrd_is_running() "
						 "   THEN "
						 "     CASE "
						 "       WHEN repmgr.repmgrd_is_paused() THEN 1 ELSE 0 "
						 "     END "
						 "   ELSE 2 "
						 " END AS repmgrd_status");
	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_db_error(conn, query.data, _("unable to execute repmgrd status query"));
	}
	else
	{
		repmgrd_status = atoi(UXSQLgetvalue(res, 0, 0));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);
	return repmgrd_status;
}


/* miscellaneous debugging functions */

const char *
print_node_status(NodeStatus node_status)
{
	switch (node_status)
	{
		case NODE_STATUS_UNKNOWN:
			return "UNKNOWN";
		case NODE_STATUS_UP:
			return "UP";
		case NODE_STATUS_SHUTTING_DOWN:
			return "SHUTTING_DOWN";
		case NODE_STATUS_DOWN:
			return "SHUTDOWN";
		case NODE_STATUS_UNCLEAN_SHUTDOWN:
			return "UNCLEAN_SHUTDOWN";
		case NODE_STATUS_REJECTED:
			return "REJECTED";
	}

	return "UNIDENTIFIED_STATUS";
}


const char *
print_uxsqlping_status(UXPing ping_status)
{
	switch (ping_status)
	{
		case UXSQLPING_OK:
			return "UXSQLPING_OK";
		case UXSQLPING_REJECT:
			return "UXSQLPING_REJECT";
		case UXSQLPING_NO_RESPONSE:
			return "UXSQLPING_NO_RESPONSE";
		case UXSQLPING_NO_ATTEMPT:
			return "UXSQLPING_NO_ATTEMPT";
	}

	return "UXSQLPING_UNKNOWN_STATUS";
}

/*
 * 适配多网卡多虚拟 IP 场景, IP 及 网卡名称分别存入各自的指针数组
 * iCount: 虚拟 IP 或网卡数目
 */
int
parse_multi_networkcard(const char *pSrc, char **pArray)
{
	int i = 0;
	int  iCount = 0; /* 解析出虚拟 IP 或网卡数目 */
	char *pTmp = strdup(pSrc);
	char *pToken = strtok(pTmp, ",");

	while (pToken != NULL && iCount < MAX_AMOUNT)
	{
		pArray[iCount] = (char *)ux_malloc0(MAX_LENGTH);
		if (pArray[iCount] == NULL)
		{
			perror("Failed to allocate memory for IP address");
			/* 释放之前分配的内存 */
			for (i = 0; i < iCount; i++)
			{
				ux_free(pArray[i]);
				pArray[i] = NULL;
			}
			return -1;
		}

		strncpy(pArray[iCount], pToken, MAX_LENGTH - 1);
		pArray[iCount][MAX_LENGTH - 1] = '\0';
		iCount++;
		pToken = strtok(NULL, ",");
	}
	ux_free(pTmp);

	return iCount;
}

/*
 * uxdb: Bind virutal IP to locale node network card
 * tianbing
 */
bool
bind_virtual_ip(const char *vip, const char *network_card)
{
	char        bind_vip[MAXLEN];
	int     r;
	int uid = getuid();

	/* if local node already has vip, don't need to execute bind command */
	if (is_exist_bind_virtual_ip(vip, network_card))
	{
		log_notice(_("locale node already bind virtual_ip info"));
		return true;
	}

	/*bind vip to network card*/
	if(uid == 0) //root user
		sprintf(bind_vip, "ip addr add %s dev %s", vip, network_card);
	else
	{
		/* modify by songjinzhou for #178952 at 2023/03/16 reveiwer houjiaxing. */
		if(!strlen(uxdb_passwd))
		{
			/* if local node already has vip, don't need to execute bind command */
			if (is_exist_bind_virtual_ip(pvipsArray[i], pNetworkcardsArray[i], uxdb_passwd))
			{
				log_notice(_("locale node already bind virtual_ip info %s dev %s"), pvipsArray[i], pNetworkcardsArray[i]);
				continue;
			}

			/*bind vip to network card*/
			if(uid == 0) //root user
				sprintf(bind_vip, "ip addr add %s dev %s", pvipsArray[i], pNetworkcardsArray[i]);
			else
			{
				/* modify by songjinzhou for #178952 at 2023/03/16 reveiwer houjiaxing. */
				if(!strlen(uxdb_passwd))
				{
					sprintf(bind_vip, "sudo ip addr add %s dev %s", pvipsArray[i], pNetworkcardsArray[i]);
				}
				else
				{
					sprintf(bind_vip, "echo '%s' | sudo -S ip addr add %s dev %s", uxdb_passwd, pvipsArray[i], pNetworkcardsArray[i]);
				}
			}
			r = ux_system(bind_vip);

			if (r != 0)
			{
				log_warning(_("unable to bind the virtual ip"));
				/* BEGIN Added by chen_jingwen for #207866 at 2025/01/14 */
				for (i = 0; i < iVipCount; i++)
				{
					ux_free(pvipsArray[i]);
					ux_free(pNetworkcardsArray[i]);
					pvipsArray[i] = NULL;
					pNetworkcardsArray[i] = NULL;
				}
				/* END Added by chen_jingwen for #207866 at 2025/01/14 */
				return false;
			}

			arping_virtual_ip();
		}
		else
		{
			sprintf(bind_vip, "echo '%s' | sudo -S ip addr add %s dev %s", uxdb_passwd, vip, network_card);
			/* BEGIN Added by chen_jingwen for #207866 at 2025/01/14 */
			pvipsArray[i] = NULL;
			pNetworkcardsArray[i] = NULL;
			/* END Added by chen_jingwen for #207866 at 2025/01/14 */
		}
	}
	r = ux_system(bind_vip);

	if (r != 0)
	{
		log_warning(_("unable to bind the virtual ip"));
		/* BEGIN Added by chen_jingwen for #207866 at 2025/01/14 */
		for (i = 0; i < iVipCount; i++)
		{
			ux_free(pvipsArray[i]);
			pvipsArray[i] = NULL;
		}
		for (i = 0; i < iNetworkcardCount; i++)
		{
			ux_free(pNetworkcardsArray[i]);
			pNetworkcardsArray[i] = NULL;
		}
		/* END Added by chen_jingwen for #207866 at 2025/01/14 */
		return false;
	}

	arping_virtual_ip();

	return true;
}

/*
 * uxdb: Unbind virutal IP to locale node network card
 * tianbing
 */
bool
unbind_virtual_ip(const char *vip, const char *network_card)
{
	char        unbind_vip[MAXLEN];
	int     r;
	int uid = getuid();

	/* if local node has not vip, don't need to execute unbind command */
	if (!is_exist_bind_virtual_ip(vip, network_card))
	{
		log_notice(_("locale node not get virtual_ip info"));
		return true;
	}

	if(uid == 0) //root user
		sprintf(unbind_vip, "ip addr del %s dev %s", vip, network_card);
	else
	{
		/* modify by songjinzhou for #178952 at 2023/03/16 reveiwer houjiaxing. */
		if(!strlen(uxdb_passwd))
		{
			/* if local node has not vip, don't need to execute unbind command */
			if (!is_exist_bind_virtual_ip(pvipsArray[i], pNetworkcardsArray[i], uxdb_passwd))
			{
				log_notice(_("locale node not get virtual_ip info %s dev %s"), pvipsArray[i], pNetworkcardsArray[i]);
				continue;
			}

			if(uid == 0) //root user
				sprintf(unbind_vip, "ip addr del %s dev %s", pvipsArray[i], pNetworkcardsArray[i]);
			else
			{
				/* modify by songjinzhou for #178952 at 2023/03/16 reveiwer houjiaxing. */
				if(!strlen(uxdb_passwd))
				{
					sprintf(unbind_vip, "sudo ip addr del %s dev %s", pvipsArray[i], pNetworkcardsArray[i]);
				}
				else
				{
					sprintf(unbind_vip, "echo '%s' | sudo -S ip addr del %s dev %s", uxdb_passwd, pvipsArray[i], pNetworkcardsArray[i]);
				}
			}

			r = ux_system(unbind_vip);

			if (r != 0)
			{
				log_warning(_("unable to unbind the virtual ip"));
				/* BEGIN Added by chen_jingwen for #207866 at 2025/01/14 */
				for (i = 0; i < iVipCount; i++)
				{
					ux_free(pvipsArray[i]);
					ux_free(pNetworkcardsArray[i]);
					pvipsArray[i] = NULL;
					pNetworkcardsArray[i] = NULL;
				}
				/* END Added by chen_jingwen for #207866 at 2025/01/14 */
				return false;
			}
		}
		else
		{
			sprintf(unbind_vip, "echo '%s' | sudo -S ip addr del %s dev %s", uxdb_passwd, vip, network_card);
			/* BEGIN Added by chen_jingwen for #207866 at 2025/01/14 */
			pvipsArray[i] = NULL;
			pNetworkcardsArray[i] = NULL;
			/* END Added by chen_jingwen for #207866 at 2025/01/14 */
		}
	}

	r = ux_system(unbind_vip);

	if (r != 0)
	{
		log_warning(_("unable to unbind the virtual ip"));
		/* BEGIN Added by chen_jingwen for #207866 at 2025/01/14 */
		for (i = 0; i < iVipCount; i++)
		{
			ux_free(pvipsArray[i]);
			pvipsArray[i] = NULL;
		}
		for (i = 0; i < iNetworkcardCount; i++)
		{
			ux_free(pNetworkcardsArray[i]);
			pNetworkcardsArray[i] = NULL;
		}
		/* END Added by chen_jingwen for #207866 at 2025/01/14 */
		return false;
	}

	return true;
}

/*
 * uxdb: Check that if virutal ip, network card has configured
 * tianbing
 */
bool
check_vip_conf(const char *vip, const char *network_card)
{
	if(!strlen(vip))
		return false;
	else
	{
		if(!strlen(network_card))
		{
			log_notice(_("network card is not configured, The configured virtual ip does not take effect.\n"));
			return false;
		}

		return true;
	}
}

/* BEGIN:  Added by huyn for #160873, 2022/9/20  reviewer:songjz */
/*
 * check whether  a virtual_ip has been bound to the local node
 */
static bool
is_exist_bind_virtual_ip(const char *vip, const char *network_card)
{
	UXSQLExpBufferData command;
	UXSQLExpBufferData command_output;
	char command_str[MAXLEN] = {0};
	int uid = 0;

	initUXSQLExpBuffer(&command);
	uid = getuid();
	if (uid == 0) 
	{
		/* root user */
		snprintf(command_str, MAXLEN,
					 " ip addr show dev %s|grep \"%s\" ", network_card , vip);
	}
	else
	{
		snprintf(command_str, MAXLEN,
					 " sudo ip addr show dev %s|grep \"%s\" ", network_card , vip);
	}
	log_notice(_("get vip command %s"), command_str);
	appendUXSQLExpBufferStr(&command,command_str);

	initUXSQLExpBuffer(&command_output);
	(void) local_command_simple(command.data,
								&command_output);
	if (command_output.len == 0)
	{
		/* BEGIN Added by chen_jingwen for #207866 at 2024/10/8 */
		/* 分配的空间需要释放，下同 */
		termUXSQLExpBuffer(&command);
		termUXSQLExpBuffer(&command_output);
		/* END Added by chen_jingwen for #207866 at 2024/10/8 */
		return false;
	}
	else
	{
		log_notice(_("bind virtual_ip info is %s"), command_output.data);
		/* BEGIN Added by chen_jingwen for #207866 at 2024/10/8 */
		termUXSQLExpBuffer(&command);
		termUXSQLExpBuffer(&command_output);
		/* END Added by chen_jingwen for #207866 at 2024/10/8 */
		return true;
	}
}
/* END:  Added by huyn for #160873, 2022/9/20  reviewer:songjz */

/*
 *  uxdb: get virtual ip
 *  tianbing
 */
bool
get_virtual_ip(UXconn *conn, int primary_id, char *virtual_ip)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
			"SELECT virtual_ip FROM repmgr.nodes n "
			" WHERE n.node_id = %i",
			primary_id);

	res = UXSQLexec(conn, query.data);
	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to get virtual ip"));
		log_detail("%s", UXSQLerrorMessage(conn));
		UXSQLclear(res);
		return false;
	}

	if (UXSQLntuples(res) == 0)
	{
		UXSQLclear(res);
		return false;
	}

	strncpy(virtual_ip, UXSQLgetvalue(res, 0, 0), MAXLEN);

	if(strlen(virtual_ip) == 0)
		return false;

	UXSQLclear(res);
	return true;
}

/*
 *  uxdb: get network card
 *  tianbing
 */
bool
get_network_card(UXconn *conn, int primary_id, char *network_card)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query,
			"SELECT network_card FROM repmgr.nodes n "
			" WHERE n.node_id = %i",
			primary_id);

	res = UXSQLexec(conn, query.data);
	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("unable to get network card"));
		UXSQLclear(res);
		return false;
	}

	if (UXSQLntuples(res) == 0)
	{
		UXSQLclear(res);
		return false;
	}

	strncpy(network_card, UXSQLgetvalue(res, 0, 0), MAXLEN);

	if(strlen(network_card) == 0)
		return false;

	UXSQLclear(res);
	return true;
}

/*
 * ux_catalog.ux_size_pretty(replication_lag)
 *
 */
void
get_ux_size_pretty(UXconn *conn, long long unsigned int lag_bytes, char *lag_str)
{
	UXresult   *res = NULL;
	UXSQLExpBufferData query;

	initUXSQLExpBuffer(&query);

	appendUXSQLExpBuffer(&query, "SELECT ux_catalog.ux_size_pretty(%llu::numeric)", lag_bytes);

	res = UXSQLexec(conn, query.data);

	termUXSQLExpBuffer(&query);

	if (UXSQLresultStatus(res) != UXRES_TUPLES_OK)
	{
		log_error(_("could not get the size"));
		log_detail("%s", UXSQLerrorMessage(conn));
	}
	else
		strcpy(lag_str, UXSQLgetvalue(res, 0, 0));

	UXSQLclear(res);
}

/* BEGIN:  Added by huyn for #177313, 2023/2/17  reviewer:wangyh,zhangwj */
/* 提升为主节点之后，要在主节点上执行一次checkpoint，
 *将新的时间线更新到control中
 */
void
new_primary_execute_checkpoint(UXconn *conn)
{
	UXSQLExpBufferData query;
	UXresult   *res = NULL;

	initUXSQLExpBuffer(&query);
	appendUXSQLExpBuffer(&query, "checkpoint;");

	log_notice(_("promotion primary node laster , execute:\n%s"), query.data);

	res = UXSQLexec(conn, query.data);

	if (UXSQLresultStatus(res) != UXRES_COMMAND_OK)
	{
		log_db_error(conn, query.data, _("new_primary_execute_checkpoint(): unable to execute query"));
	}

	termUXSQLExpBuffer(&query);
	UXSQLclear(res);
	return;
}
/* END:  Added by huyn for #177313, 2023/2/17  reviewer:wangyh,zhangwj */

static void
arping_virtual_ip(void)
{
	UXSQLExpBufferData arping_command_str;
	int ret = -1;

	if (strlen(config_file_options.arping_command) > 0)
	{
		log_notice("arping virtual ip...");
		initUXSQLExpBuffer(&arping_command_str);

		if(strlen(config_file_options.uxdb_password) > 0)
			appendUXSQLExpBuffer(&arping_command_str, "echo '%s' | sudo -S ",
				config_file_options.uxdb_password);
		appendUXSQLExpBuffer(&arping_command_str, "%s",
							 config_file_options.arping_command);

		ret = ux_system(arping_command_str.data);
		termUXSQLExpBuffer(&arping_command_str);

		if (ret != 0)
			log_warning(_("unable to exec arping, exec failed ret: %d"), ret);
	}
	else
		log_debug(_("arping will not execute because the command is empty"));
}
