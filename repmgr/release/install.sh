#! /bin/bash
VERSION=5.0
LOCAL_PATH=./repmgr-$VERSION

OS_NAME=`uname -s`
OS_VER=`uname -r`
OS_PLATFORM=`uname -i`
BUILD_OS_PLATFORM=x86_64

DBSQL_INSTALL_PATH=$(ux_config --bindir)/..
INSTALL_LOG=repmgr_install_`date +%Y_%m_%d_%H%M%S`.log
touch $INSTALL_LOG

##########################
# Check System Information
#  - Check OS Name & OS Version
##################################

check_os_info()
{
	# Check OS Name
	if [ "$OS_NAME" != "Linux" ]; then
		echo -e "Error : It is only for Linux."
		echo -e "        Exit installation.."
		echo -e "Error : Unsupported OS Name $OS_NAME."
		exit 1;
	fi

	# Check OS Version
	IFSUPPORT=`grep ${OS_VER:0:6} platform`

	if [ -z "$IFSUPPORT" ]; then
		echo -e "Error : Unsupported OS Release Version $OS_VER."
		echo -e "        Exit installation.."
		echo -e "Error : Unsupported OS Release Version $OS_VER."
		exit 1;
	fi
	
	# Check OS Platform
	if [ "$OS_PLATFORM" != "$BUILD_OS_PLATFORM" ]; then
		echo -e "Error : Unsupported OS PLATFORM $OS_PLATFORM."
		echo -e "        Exit installation.."
		echo -e "Error : Unsupported OS PLATFORM $OS_PLATFORM."
		exit 1;
	fi

	echo -e "  .. Target OS : $OS_NAME $OS_VER $OS_PLATFORM\n"
}

# Temporarily no need to detect OS information
# check_os_info

# repmgr install optional
read -p "Do you want to install repmgr?[Y/N]:" inputInstallrepmgr
if [ -z "$inputInstallrepmgr" ] ; then
	inputInstallrepmgr=Y
fi

if [[ "$inputInstallrepmgr" == "Y" || "$inputInstallrepmgr" == "y" ]] ; then
	if [ "${DBSQL_INSTALL_PATH}" = "/.." ] ; then
		echo "'ux_config' not found, please first configure UXDB environment variable" | tee -a $INSTALL_LOG
		exit 1;
	fi

	tar -zxf repmgr-$VERSION.tar
	mkdir -p $DBSQL_INSTALL_PATH/lib
	mkdir -p $DBSQL_INSTALL_PATH/share/extension
	mkdir -p $DBSQL_INSTALL_PATH/share/extension_compatible
	mkdir -p $DBSQL_INSTALL_PATH/share/extension_mysql
	mkdir -p $DBSQL_INSTALL_PATH/bin

	install -c -m 755  $LOCAL_PATH/lib/* $DBSQL_INSTALL_PATH/lib
	echo "${DBSQL_INSTALL_PATH}/lib/repmgr.so" >> $INSTALL_LOG
	install -c -m 644  $LOCAL_PATH/extension/* $DBSQL_INSTALL_PATH/share/extension
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.0--4.1.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.0.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.1--4.2.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.1.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.2--4.3.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.2.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.3.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--4.4.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--5.0.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr--unpackaged--4.0.sql" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/share/extension/repmgr.control" >> $INSTALL_LOG
	install -c -m 755 $LOCAL_PATH/bin/repmgr $LOCAL_PATH/bin/repmgrd $LOCAL_PATH/bin/repmgr_service $LOCAL_PATH/bin/uxdb_service $DBSQL_INSTALL_PATH/bin
	install -c -m 644 $LOCAL_PATH/bin/repmgr.conf $DBSQL_INSTALL_PATH/bin
	echo "${DBSQL_INSTALL_PATH}/bin/repmgr" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/bin/repmgrd" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/bin/repmgr.conf" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/bin/repmgr_service" >> $INSTALL_LOG
	echo "${DBSQL_INSTALL_PATH}/bin/uxdb_service" >> $INSTALL_LOG

	rm -rf $LOCAL_PATH

else
	echo -e "Not Install!" | tee -a $INSTALL_LOG
	exit 1;
fi

echo -e "Install repmgr success!" | tee -a $INSTALL_LOG
echo -e "See the installation log for details [$INSTALL_LOG]!"