###################################################
# repmgr sample configuration file
###################################################

# Some configuration items will be set with a default value; this
# is noted for each item. Where no default value is shown, the
# parameter will be treated as empty or false.
#
# IMPORTANT: string values can be provided as-is, or enclosed in single quotes
# (but not double-quotes, which will be interpreted as part of the string),
# e.g.:
#
#  node_name=foo
#  node_name = 'foo'
#
# =============================================================================
# Required configuration items
# =============================================================================
#
# repmgr and repmgrd require the following items to be explicitly configured.


node_id=2    	     	     	 # A unique integer greater than zero
node_name='node2'			 # An arbitrary (but unique) string; we recommend
				 # using the server's hostname or another identifier
				 # unambiguously associated with the server to avoid
				 # confusion. Avoid choosing names which reflect the
				 # node's current role, e.g. 'primary' or 'standby1',
				 # as roles can change and it will be confusing if
				 # the current primary is called 'standby1'.
                                 # The string's maximum length is 63 characters and it should
                                 # contain only printable ASCII characters.

conninfo='host=192.71.0.115 user=repmgr dbname=repmgr connect_timeout=2 sql_exec_timeout=300'	 # Database connection information as a conninfo string.
				 # All servers in the cluster must be able to connect to
				 # the local node using this string.
				 #
				 # For details on conninfo strings, see:
				 #  https://www.uxsino.com/docs/current/libuxsql-connect.html#LIBUXSQL-CONNSTRING
				 #
				 # If repmgrd is in use, consider explicitly setting
				 # "connect_timeout" in the conninfo string to determine
				 # the length of time which elapses before a network
				 # connection attempt is abandoned; for details see:
				 #  https://www.uxsino.com/docs/current/libuxsql-connect.html#LIBUXSQL-CONNECT-CONNECT-TIMEOUT

data_directory='/home/uxdb/uxdbinstall/dbsql/bin/datarep'		 # The node's data directory. This is needed by repmgr
				 # when performing operations when the UXsinoDB instance
				 # is not running and there's no other way of determining
				 # the data directory.


# =============================================================================
# Optional configuration items
# =============================================================================


#------------------------------------------------------------------------------
# Server settings
#------------------------------------------------------------------------------

#config_directory=''		 # If configuration files are located outside the data
				 # directory, specify the directory where the main
				 # uxsinodb.conf file is located.

#------------------------------------------------------------------------------
# Replication settings
#------------------------------------------------------------------------------

#replication_user='repmgr'	 # User to make replication connections with, if not set
				 # defaults to the user defined in "conninfo".

#replication_type='physical'	 # Must "physical" (the default).

#location=default		 # arbitrary string defining the location of the node; this
				 # is used during failover to check visibilty of the
				 # current primary node. See the 'repmgrd' documentation
				 # in README.md for further details.

#use_replication_slots=no	 # whether to use physical replication slots
				 # NOTE: when using replication slots,
				 # 'max_replication_slots' should be configured for
				 # at least the number of standbys which will connect
				 # to the primary.

#------------------------------------------------------------------------------
# Witness server settings
#------------------------------------------------------------------------------

#witness_sync_interval=15	 # interval (in seconds) to synchronise node records
				 # to the witness server

#------------------------------------------------------------------------------
# Logging settings
#------------------------------------------------------------------------------
#
# Note that logging facility settings will only apply to `repmgrd` by default;
# `repmgr` will always write to STDERR unless the switch `--log-to-file` is
# supplied, in which case it will log to the same destination as `repmgrd`.
# This is mainly intended for those cases when `repmgr` is executed directly
# by `repmgrd`.

log_level=DEBUG			 # Log level: possible values are DEBUG, INFO, NOTICE,
				 # WARNING, ERROR, ALERT, CRIT or EMERG

#log_facility=STDERR		 # Logging facility: possible values are STDERR, or for
				 # syslog integration, one of LOCAL0, LOCAL1, ..., LOCAL7, USER

log_file='/home/uxdb/uxdbinstall/dbsql/bin/logrepmgrd.log'			 # STDERR can be redirected to an arbitrary file
#log_status_interval=300	 # interval (in seconds) for repmgrd to log a status message


#------------------------------------------------------------------------------
# Event notification settings
#------------------------------------------------------------------------------

# event notifications can be passed to an arbitrary external program
# together with the following parameters:
#
#   %n - node ID
#   %e - event type
#   %s - success (1 or 0)
#   %t - timestamp
#   %d - details
#
# the values provided for "%t" and "%d" will probably contain spaces,
# so should be quoted in the provided command configuration, e.g.:
#
#   event_notification_command='/path/to/some/script %n %e %s "%t" "%d"'
#
# By default, all notifications will be passed; the notification types
# can be filtered to explicitly named ones, e.g.:
#
#   event_notifications=primary_register,standby_register

#event_notification_command=''		# An external program or script which
					# can be executed by the user under which
					# repmgr/repmgrd are run.

#event_notifications=''			# A commas-separated list of notification
					# types

#------------------------------------------------------------------------------
# Environment/command settings
#------------------------------------------------------------------------------

ux_bindir='/home/uxdb/uxdbinstall/dbsql/bin'				# Path to UXsinoDB binary directory (location
					# of ux_ctl, ux_basebackup etc.). Only needed
					# if these files are not in the system $PATH.
					#
					# Debian/Ubuntu users: you will probably need to
					# set this to the directory where `ux_ctl` is located,
					# e.g. /usr/lib/uxsinodb/9.6/bin/
					#
					# *NOTE* "ux_bindir" is only used when repmgr directly
					# executes UXsinoDB binaries; any user-defined scripts
					# *must* be specified with the full path

repmgr_bindir='/home/uxdb/uxdbinstall/dbsql/bin'			# Path to repmgr binary directory (location of the repmgr
					# binary. Only needed if the repmgr executable is not in
                                        # the system $PATH or the path defined in "ux_bindir".

#use_primary_conninfo_password=false	# explicitly set "password" in recovery.conf's
					# "primary_conninfo" parameter using the value contained
					# in the environment variable UXPASSWORD
#passfile=''				# path to .uxpass file to include in "primary_conninfo"
#------------------------------------------------------------------------------
# external command options
#------------------------------------------------------------------------------
#
# Options which can be passed to external commands invoked by repmgr/repmgrd.
#
# Examples:
#
#   ux_ctl_options='-s'
#   ux_basebackup_options='--label=repmgr_backup'
#   rsync_options=--archive --checksum --compress --progress --rsh="ssh -o \"StrictHostKeyChecking no\""
#   ssh_options=-o "StrictHostKeyChecking no"

#ux_ctl_options='' 			# Options to append to "ux_ctl"
#ux_basebackup_options=''		# Options to append to "ux_basebackup"
#rsync_options=''			# Options to append to "rsync"
ssh_options='-q -o ConnectTimeout=10'	# Options to append to "ssh"



#------------------------------------------------------------------------------
# "standby clone" settings
#------------------------------------------------------------------------------
#
# These settings apply when cloning a standby ("repmgr standby clone").
#
# Examples:
#
#   tablespace_mapping=/path/to/original/tablespace=/path/to/new/tablespace
#   restore_command = 'cp /path/to/archived/wals/%f %p'

#tablespace_mapping=''	  		# Tablespaces can be remapped from one
					# file system location to another. This
					# parameter can be provided multiple times.

#restore_command=''			# This will be placed in the recovery.conf file generated
					# by repmgr.

#archive_cleanup_command=''		# This will be placed in the recovery.conf file generated
					# by repmgr. Note we recommend using Barman for managing
					# WAL archives (see: https://www.uxbarman.org )

#recovery_min_apply_delay=		# If provided, "recovery_min_apply_delay" in recovery.conf
					# will be set to this value (UXsinoDB 9.4 and later).


#------------------------------------------------------------------------------
# "standby promote" settings
#------------------------------------------------------------------------------

# These settings apply when instructing a standby to promote itself to the
# new primary ("repmgr standby promote").

#promote_check_timeout=60		# The length of time (in seconds) to wait
					# for the new primary to finish promoting
#promote_check_interval=1		# The interval (in seconds) to check whether
					# the new primary has finished promoting


#------------------------------------------------------------------------------
# "standby follow" settings
#------------------------------------------------------------------------------

# These settings apply when instructing a standby to follow the new primary
# ("repmgr standby follow").

#primary_follow_timeout=60		# The max length of time (in seconds) to wait
					# for the new primary to become available
#standby_follow_timeout=15		# The max length of time (in seconds) to wait
					# for the standby to connect to the primary
#standby_follow_restart=false		# Restart the standby instead of sending a SIGHUP
					# (only for UxsinoDB 13 and later)

#------------------------------------------------------------------------------
# "standby switchover" settings
#------------------------------------------------------------------------------

# These settings apply when switching roles between a primary and a standby
# ("repmgr standby switchover").

#shutdown_check_timeout=60		# The max length of time (in seconds) to wait for the demotion
					# candidate (current primary) to shut down
#standby_reconnect_timeout=60		# The max length of time (in seconds) to wait
					# for the demoted standby to reconnect to the promoted
					# primary (note: this value should be equal to or greater
					# than that set for "node_rejoin_timeout")
#wal_receive_check_timeout=30		# The max length of time (in seconds) to wait for the walreceiver
					# on the standby to flush WAL to disk before comparing location
					# with the shut-down primary

#------------------------------------------------------------------------------
# "node rejoin" settings
#------------------------------------------------------------------------------

# These settings apply when reintegrating a node into a replication cluster
# with "repmgrd_node_rejoin"

#node_rejoin_timeout=60		# The maximum length of time (in seconds) to wait for
					# the node to reconnect to the replication cluster

#------------------------------------------------------------------------------
# Barman options
#------------------------------------------------------------------------------

#barman_server=''			# The barman configuration section
#barman_host=''				# The host name of the barman server
#barman_config=''			# The Barman configuration file on the
					# Barman server (needed if the file is
					# in a non-standard location)

#------------------------------------------------------------------------------
# Failover and monitoring settings (repmgrd)
#------------------------------------------------------------------------------
#
# These settings are only applied when repmgrd is running. Values shown
# are defaults.

failover=automatic			# one of 'automatic', 'manual'.
					# determines what action to take in the event of upstream failure
					#
					# 'automatic': repmgrd will automatically attempt to promote the
					#    node or follow the new upstream node
					# 'manual': repmgrd will take no action and the node will require
					#    manual attention to reattach it to replication

priority=100				# indicates a preferred priority for promoting nodes;
					# a value of zero prevents the node being promoted to primary
					# (default: 100)

connection_check_type=ping		# How to check availability of the upstream node; valid options:
                                        #  'ping': use UXSQLping() to check if the node is accepting connections
                                        #  'connection': execute a throwaway query on the current connection
reconnect_attempts=3			# Number of attempts which will be made to reconnect to an unreachable
					# primary (or other upstream node)
reconnect_interval=5			# Interval between attempts to reconnect to an unreachable
					# primary (or other upstream node)
promote_command='repmgr -f /home/uxdb/uxdbinstall/dbsql/bin/repmgr.conf standby promote --log-to-file'			# command repmgrd executes when promoting a new primary; use something like:
					#
					#     repmgr standby promote -f /etc/repmgr.conf
					#
follow_command='repmgr standby follow -f /home/uxdb/uxdbinsatll/dbsql/bin/repmgr.conf --log-to-file --upstream-node-id=%n'			# command repmgrd executes when instructing a standby to follow a new primary;
					# use something like:
					#
					#     repmgr standby follow -f /etc/repmgr.conf -W --upstream-node-id=%n
					#
#primary_notification_timeout=60	# Interval (in seconds) which repmgrd on a standby
					# will wait for a notification from the new primary,
					# before falling back to degraded monitoring
#repmgrd_standby_startup_timeout=60	# Interval (in seconds) which repmgrd on a standby will wait
					# for the the local node to restart and become ready to accept connections after
					# executing "follow_command" (defaults to the value set in "standby_reconnect_timeout")

monitoring_history=yes                  # Whether to write monitoring data to the "montoring_history" table
monitor_interval_secs=2                # Interval (in seconds) at which to write monitoring data
#degraded_monitoring_timeout=-1		# Interval (in seconds) after which repmgrd will terminate if the
					# server(s) being monitored are no longer available. -1 (default)
					# disables the timeout completely.
#async_query_timeout=60			# Interval (in seconds) which repmgrd will wait before
					# cancelling an asynchronous query.
#repmgrd_pid_file=			# Path of PID file to use for repmgrd; if not set, a PID file will
					# be generated in a temporary directory specified by the environment
					# variable $TMPDIR, or if not set, in "/tmp". This value can be overridden
					# by the command line option "-p/--pid-file"; the command line option
					# "--no-pid-file" will force PID file creation to be skipped.
					# Note: there is normally no need to set this, particularly if
					# repmgr was installed from packages.
standby_disconnect_on_failover=true	# If "true", in a failover situation wait for all standbys to
					# disconnect their WAL receivers before electing a new primary
					# (UXsinoDB 9.5 and later only; repmgr user must be a superuser for this)
#sibling_nodes_disconnect_timeout=30	# If "standby_disconnect_on_failover" is true, the maximum length of time
					#  (in seconds) to wait for other standbys to confirm they have disconnected their
					# WAL receivers
#primary_visibility_consensus=false	# If "true", only continue with failover if no standbys have seen
					# the primary node recently. *Must* be the same on all nodes.
#always_promote=false			# Always promote a node, even if repmgr metadata is outdated
#failover_validation_command=''		# Script to execute for an external mechanism to validate the failover
					# decision made by repmgrd. One or both of the following parameter placeholders
					# should be provided, which will be replaced by repmgrd with the appropriate
					# value: %n (node_id), %a (node_name). *Must* be the same on all nodes.
#election_rerun_interval=15		# if "failover_validation_command" is set, and the command returns
					# an error, pause the specified amount of seconds before rerunning the election.
					#
					# The following items are relevant for repmgrd running on the primary,
					# and will be ignored on non-primary nodes
#child_nodes_check_interval=5		# Interval (in seconds) to check for attached child nodes (standbys)
#child_nodes_connected_min_count=-1	# Minimum number of child nodes which must remain connected, otherwise
					# disconnection command will be triggered
#child_nodes_disconnect_min_count=-1	# Minimum number of disconnected child nodes required to execute disconnection command
					# (ignored if "child_nodes_connected_min_count" set)
#child_nodes_disconnect_timeout=30	# Interval between child node disconnection and disconnection command execution
#child_nodes_disconnect_command=''	# Command to execute if child node disconnection detected


#root_password=''			 # root password
#uxdb_password=''			 # uxdb password
#try_synchronous_connection_timeout=30	 # sync <-> async: try synchronous connection timeout. see check_sync_async

#------------------------------------------------------------------------------
# service control commands
#------------------------------------------------------------------------------
#
# repmgr provides options to override the default ux_ctl commands
# used to stop, start, restart, reload and promote the UXsinoDB cluster
#
# These options are useful when UXsinoDB has been installed from a package
# which provides OS-level service commands. In environments using an init system
# such as systemd, which keeps track of the state of various services, it is
# essential that the service commands are correctly configured and ux_ctl is
# not executed directly.
#
# NOTE: These commands must be runnable on remote nodes as well for switchover
# to function correctly.
#
# If you use sudo, the user repmgr runs as (usually 'uxdb')  must have
# passwordless sudo access to execute the command.
#
# For example, to use systemd, you can set
#
#    service_start_command = 'sudo systemctl start uxsinodb-9.6'
#    (...)
#
# and then use the following sudoers configuration:
#
#    # this is required when running sudo over ssh without -t:
#    Defaults:uxdb !requiretty
#    uxdb ALL = NOPASSWD: /usr/bin/systemctl stop uxsinodb-9.6, \
#       /usr/bin/systemctl start uxsinodb-9.6, \
#       /usr/bin/systemctl restart uxsinodb-9.6
#
# Debian/Ubuntu users: use "sudo ux_ctlcluster" to execute service control commands.
#
# For more details, see: https://repmgr.org/docs/current/configuration-service-commands.html

service_start_command = '/home/uxdb/uxdbinstall/dbsql/bin/ux_ctl -D /home/uxdb/uxdbinstall/dbsql/bin/datarep -l logfile start'
service_stop_command = '/home/uxdb/uxdbinstall/dbsql/bin/ux_ctl -D /home/uxdb/uxdbinstall/dbsql/bin/datarep -l logfile stop'
service_restart_command = '/home/uxdb/uxdbinstall/dbsql/bin/ux_ctl -D /home/uxdb/uxdbinstall/dbsql/bin/datarep -l logfile restart'
service_reload_command = '/home/uxdb/uxdbinstall/dbsql/bin/ux_ctl -D /home/uxdb/uxdbinstall/dbsql/bin/datarep -l logfile reload'
#service_promote_command = ''		# This parameter is intended for systems which provide a
					# package-level promote command, such as Debian's
					# "ux_ctlcluster". *IMPORTANT*: it is *not* a substitute
					# for "promote_command"; do not use "repmgr standby promote"
					# (or a script which executes "repmgr standby promote") here.

# Used by "repmgr daemon (start|stop)" to control repmgrd
#
repmgrd_service_start_command = 'repmgrd -f /home/uxdb/uxdbinstall/dbsql/bin/repmgr.conf -d'
repmgrd_service_stop_command = 'kill `cat /tmp/repmgrd.pid`'

#------------------------------------------------------------------------------
# Status check thresholds
#------------------------------------------------------------------------------

# Various warning/critical thresholds used by "repmgr node check".

#archive_ready_warning=16  	      	# repmgr node check --archive-ready
#archive_ready_critical=128		#
					# Numbers of files pending archiving via UXsinoDB's
					# "archive_command" configuration parameter. If
					# files can't be archived fast enough, or the archive
					# command is failing, the buildup of files can
					# cause various issues, such as server shutdown being
					# delayed until all files are archived, or excessive
					# space being occupied by unarchived files.
					#
					# Note that these values will be checked when executing
					# "repmgr standby switchover" to warn about potential
					# issues with shutting down the demotion candidate.

#replication_lag_warning=300		# repmgr node check --replication-lag
#replication_lag_critical=600		#
					# Note that these values will be checked when executing
					# "repmgr standby switchover" to warn about potential
					# issues with shutting down the demotion candidate.

#------------------------------------------------------------------------------
# Virtual IP control settings
#------------------------------------------------------------------------------
#
# Virtual IP is the ip that stream replication cluster serving external applications,
# when autofailover is occurring, Virtual IP can switch to new pirmary.
#

virtual_ip = '192.71.0.190'
network_card = 'ens33'

#------------------------------------------------------------------------------
# Disk check settings
#------------------------------------------------------------------------------
#
# primary monitoring loop would check disk status by touching file
# And in case the mounting storage is not accessible, the logic may be hung
# So set the device_check_timeout to interrupt
# device_check_times is the max times to try to check disk if check disk failure
#
#
#device_check_timeout=60  ##the default value is 60s
#device_check_times=3     ##the default value is 3 times

#------------------------------------------------------------------------------
# Standby wait timeout
#------------------------------------------------------------------------------
#
# When standby booting up, repmgrd would check if there is primary node.
# If no, standby would wait for primary accessable.
# If no primary within standby_wait_timeout, standby would try to do failover

#standby_wait_timeout=10  ##default is 10 mins

#------------------------------------------------------------------------------
# Check Brain Split
# ------------------------------------------------------------------------------
#
# When this option is enabled, the primary node would timely check if the
# cluster is brain split or not. Once brain split is detacted, repmgrd would
# try to resolve it or stop the DB depends.
# If the lsn on primary nodes were not split into two different paths,
# repmgrd would stop one of the primary node and do rejoin.
# Otherwise, repmgrd on each of the primary node would stop DB and repmgrd
# then wait for manually handling.

check_brain_split=true ##defalut is false, to enable the function set to true

