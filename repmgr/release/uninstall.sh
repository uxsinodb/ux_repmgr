#! /bin/bash

DBSQL_INSTALL_PATH=$(ux_config --bindir)/..
UNINSTALL_LOG=repmgr_uninstall_`date +%Y_%m_%d_%H%M%S`.log
touch $UNINSTALL_LOG


## uninstall UxdbEncryptLicense optional
read -p "Do you want to uninstall repmgr?[Y/N]:" inputUninstallrepmgr
if [ -z "$inputUninstallrepmgr" ] ; then
        inputUninstallrepmgr=Y
fi

if [[ "$inputUninstallrepmgr" == "Y" || "$inputUninstallrepmgr" == "y" ]] ; then
	if [ "${DBSQL_INSTALL_PATH}" = "/.." ] ; then
		echo "'ux_config' not found, please first configure UXDB environment variable" | tee -a $UNINSTALL_LOG
		exit 1;
	fi

	rm -rf $DBSQL_INSTALL_PATH/lib/repmgr.so
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.0--4.1.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.0.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.1--4.2.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.1.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.2--4.3.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.2.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.3.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--4.4.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--5.0.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr--unpackaged--4.0.sql
	rm -rf $DBSQL_INSTALL_PATH/share/extension/repmgr.control
	rm -rf $DBSQL_INSTALL_PATH/bin/repmgr
	rm -rf $DBSQL_INSTALL_PATH/bin/repmgrd
	rm -rf $DBSQL_INSTALL_PATH/bin/repmgr.conf
	rm -rf $DBSQL_INSTALL_PATH/bin/repmgr_service
	rm -rf $DBSQL_INSTALL_PATH/bin/uxdb_service
	echo "rm ${DBSQL_INSTALL_PATH}/lib/repmgr.so" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.0--4.1.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.0.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.1--4.2.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.1.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.2--4.3.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.2.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.3.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.4.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--5.0.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr--unpackaged--4.0.sql" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/share/extension/repmgr.control" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/bin/repmgr" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/bin/repmgrd" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/bin/repmgr.conf" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/bin/repmgr_service" >> $UNINSTALL_LOG
	echo "rm ${DBSQL_INSTALL_PATH}/bin/uxdb_service" >> $UNINSTALL_LOG
	echo -e "UNINSTALL END!" | tee -a $UNINSTALL_LOG

else
	echo -e "Not Install!"
	exit 1;
fi

echo -e "Unnstall repmgr success!" | tee -a $UNINSTALL_LOG
echo -e "See the installation log for details [$UNINSTALL_LOG]!"