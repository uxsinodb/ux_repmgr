Fencing a failed master node with repmgrd and uxbouncer
=======================================================

With automatic failover, it's essential to ensure that a failed primary
remains inaccessible to your application, even if it comes back online
again, to avoid a split-brain situation.

By using `uxbouncer` together with `repmgrd`, it's possible to combine
automatic failover with a process to isolate the failed primary from
your application and ensure that all connections which should go to
the primary are directed there smoothly without having to reconfigure
your application. (Note that as a connection pooler, `uxbouncer` can
benefit your application in other ways, but those are beyond the scope
of this document).

* * *

> *WARNING*: automatic failover is tricky to get right. This document
> demonstrates one possible implementation method, however you should
> carefully configure and test any setup to suit the needs of your own
> replication cluster/application.

* * *

In a failover situation, `repmgrd` promotes a standby to primary by executing
the command defined in `promote_command`. Normally this would be something like:

    repmgr standby promote -f /etc/repmgr.conf

By wrapping this in a custom script which adjusts the `uxbouncer` configuration
on all nodes, it's possible to fence the failed primary and redirect write
connections to the new primary.

The script consists of two sections:

* the promotion command itself
* commands to reconfigure `uxbouncer` on all nodes

Note that it requires password-less SSH access from the `repmgr` nodes
to all the `uxbouncer` nodes to be able to update the `uxbouncer`
configuration files.

For the purposes of this demonstration, we'll assume there are 3 nodes (primary
and two standbys), with `uxbouncer` listening on port 6432 handling connections
to a database called `appdb`.  The `uxdb` system user must have write
access to the `uxbouncer` configuration files on all nodes. We'll assume
there's a main `uxbouncer` configuration file, `/etc/uxbouncer.ini`, which uses
the `%include` directive (available from UxBouncer 1.6) to include a separate
configuration file, `/etc/uxbouncer.database.ini`, which will be modified by
`repmgr`.

* * *

> *NOTE*: in this self-contained demonstration, `uxbouncer` is running on the
> database servers, however in a production environment it will make more
> sense to run `uxbouncer` on either separate nodes or the application server.

* * *

`/etc/uxbouncer.ini` should look something like this:

    [uxbouncer]

    logfile = /var/log/uxbouncer/uxbouncer.log
    pidfile = /var/run/uxbouncer/uxbouncer.pid

    listen_addr = *
    listen_port = 6532
    unix_socket_dir = /tmp

    auth_type = trust
    auth_file = /etc/uxbouncer.auth

    admin_users = uxdb
    stats_users = uxdb

    pool_mode = transaction

    max_client_conn = 100
    default_pool_size = 20
    min_pool_size = 5
    reserve_pool_size = 5
    reserve_pool_timeout = 3

    log_connections = 1
    log_disconnections = 1
    log_pooler_errors = 1

    %include /etc/uxbouncer.database.ini

The actual script is as follows; adjust the configurable items as appropriate:

`/var/lib/uxdb/repmgr/promote.sh`


    #!/usr/bin/env bash
    set -u
    set -e

    # Configurable items
    UXBOUNCER_HOSTS="node1 node2 node3"
    UXBOUNCER_DATABASE_INI="/etc/uxbouncer.database.ini"
    UXBOUNCER_DATABASE="appdb"
    UXBOUNCER_PORT=6432

    REPMGR_DB="repmgr"
    REPMGR_USER="repmgr"

    # 1. Promote this node from standby to primary

    repmgr standby promote -f /etc/repmgr.conf

    # 2. Reconfigure uxbouncer instances

    UXBOUNCER_DATABASE_INI_NEW="/tmp/uxbouncer.database.ini"

    for HOST in $UXBOUNCER_HOSTS
    do
        # Recreate the uxbouncer config file
        echo -e "[databases]\n" > $UXBOUNCER_DATABASE_INI_NEW

        uxsql -d $REPMGR_DB -U $REPMGR_USER -t -A \
          -c "SELECT '${UXBOUNCER_DATABASE}-rw= ' || conninfo || ' application_name=uxbouncer_${HOST}' \
              FROM repmgr.nodes \
              WHERE active = TRUE AND type='primary'" >> $UXBOUNCER_DATABASE_INI_NEW

        uxsql -d $REPMGR_DB -U $REPMGR_USER -t -A \
          -c "SELECT '${UXBOUNCER_DATABASE}-ro= ' || conninfo || ' application_name=uxbouncer_${HOST}' \
              FROM repmgr.nodes \
              WHERE node_name='${HOST}'" >> $UXBOUNCER_DATABASE_INI_NEW


        rsync $UXBOUNCER_DATABASE_INI_NEW $HOST:$UXBOUNCER_DATABASE_INI

        uxsql -tc "reload" -h $HOST -p $UXBOUNCER_PORT -U uxdb uxbouncer

    done

    # Clean up generated file
    rm $UXBOUNCER_DATABASE_INI_NEW

    echo "Reconfiguration of uxbouncer complete"

Script and template file should be installed on each node where `repmgrd` is running.

Finally, set `promote_command` in `repmgr.conf` on each node to
point to the custom promote script:

    promote_command=/var/lib/uxdb/repmgr/promote.sh

and reload/restart any running `repmgrd` instances for the changes to take
effect.
