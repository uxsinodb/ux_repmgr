/*
 * repmgrd.h
 * Portions Copyright (c) 2016-2022, Beijing Uxsino Software Limited, Co.
 * Copyright (c) 2009-2020, UXDB Software Co.,Ltd.
 */


#ifndef _REPMGRD_H_
#define _REPMGRD_H_

#include <time.h>
#include "portability/instr_time.h"

#define OPT_NO_PID_FILE                  1000
#define OPT_DAEMONIZE                    1001

extern volatile sig_atomic_t got_SIGHUP;
extern MonitoringState monitoring_state;
extern instr_time degraded_monitoring_start;

extern t_node_info local_node_info;
extern UXconn *local_conn;
extern bool startup_event_logged;
extern char pid_file[MAXUXPATH];
extern char *config_file;   /* add by houjiaxing for #191581, 2023/11/23  reviewer:wangbocai */

bool		check_upstream_connection(UXconn **conn, const char *conninfo, UXconn **paired_conn);
void		try_reconnect(UXconn **conn, t_node_info *node_info);

int			calculate_elapsed(instr_time start_time);
const char *print_monitoring_state(MonitoringState monitoring_state);

void		update_registration(UXconn *conn);
void		terminate(int retval);
#endif							/* _REPMGRD_H_ */
