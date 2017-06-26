#!/bin/bash

SPARK_VERSION='spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz'

# Prepare the system to run this script.
init() {
    apt-get -y update
    apt-get -y install tmux jq curl wget tar bc

    # This is the  mount point for the master's /root/spark NFS share.    
    mkdir -p /root/spark/data
    
    # Check if password authentication is enabled.
    grep '^PasswordAuth' /etc/ssh/sshd_config | grep yes
    if [ $? -eq 0 ]; then
        printf "\nSECURITY WARNING: Password authentication for SSH is enabled.\n" \
            "This is a security risk, but this script won't disable it automatically to avoid the risk of " \
            "leaving you without any SSH access.\n" \
            "Please configure SSH key based authentication for this machine by following \n" \
            "https://www.linode.com/docs/security/securing-your-server \n" \
            "and then run ./slave.sh secure"
    fi
}

install_slave() {
    install_slave_node_prerequisites
    
    install_recommender_app
    
    install_spark "/root/spark/stockspark"
}



install_slave_node_prerequisites() {
    apt-get -y update
    apt-get -y install tmux openjdk-8-jre-headless dstat git
}

install_recommender_app() {
    # This is just to get the spark configuration files under deploy/.
    git clone https://github.com/pathbreak/content-recommender-spark-lda /root/spark/recommender
}


# $1 -> Target installation directory where Spark will be installed
install_spark() {
	if [ -z "$1" ]; then
		echo "Error: Missing target directory"
		return 1
	fi
        
    local target_dir="$1"
    mkdir -p "$target_dir"
    
    # Get the Apache mirror path.
    cd "$target_dir"
    local mirror_info=$(curl "https://www.apache.org/dyn/closer.lua/spark/$SPARK_VERSION?as_json=1")
    local spark_url="$(echo "$mirror_info" | jq --raw-output '.preferred')"
    local spark_path="$(echo "$mirror_info" | jq --raw-output '.path_info')"
    spark_url="$spark_url$spark_path"
    echo "Downloading: $spark_url"
    wget -O spark.tgz "$spark_url" 
    tar -xzv -f spark.tgz
    
    local archive_root_dir="$(tar -tzf spark.tgz|head -1|sed 's|/.*||')"
    local installed_dir="$(echo "$target_dir/$archive_root_dir"|tr -s '/')"
    
    cp "/root/spark/recommender/deploy/spark-defaults.conf" "$archive_root_dir/conf/"
    cp "/root/spark/recommender/deploy/metrics.properties" "$archive_root_dir/conf/"
    
    echo "Spark installed in: $installed_dir"
}

# Starts the Spark slave daemon on this machine's private IP address.
#   $1 -> The directory where a spark installation exists.
#   $2 -> The private IP address of cluster's master node.
join_cluster() {
    local spark_dir="$1"
    if [ ! -f "$spark_dir/sbin/start-slave.sh" ]; then
        echo "Error: $spark_dir does not seem to be a Spark installation."
        return 1
    fi

    local master_ip="$2"
    
    setup_nfs_shares "$master_ip"

    # Master daemon uses SPARK_LOCAL_IP only for port 8080 (WebUI), 
    # and --host for ports 6066 (REST endpoint) and 7077 (service)
    local private_ip=$(ip addr | grep 'eth0:1' | awk '{print $2}'|tr  '/' ' ' | awk '{print $1}')
    
    SPARK_LOCAL_IP=$private_ip SPARK_PUBLIC_DNS=$private_ip  \
        "$spark_dir/sbin/start-slave.sh" "--port 7078" \
        "--host $private_ip" "spark://$master_ip:7077"     
}


# Stops the Spark slave daemon on this machine.
# Does not remove the NFS mount to master.
#   $1 -> The directory where a spark installation exists.
leave_cluster() {
    local spark_dir="$1"
    if [ ! -f "$spark_dir/sbin/stop-slave.sh" ]; then
        echo "Error: $spark_dir does not seem to be a Spark installation."
        return 1
    fi

    "$spark_dir/sbin/stop-slave.sh"
}


secure() {
    # Disable PasswordAuthentication for ssh daemon.
    sed -i 's/^PasswordAuthentication.*$/PasswordAuthentication no/' /etc/ssh/sshd_config
    service ssh restart
}

# Mount the spark master's NFS shared data directory locally.
#   $1 -> Spark Master's private IP address
setup_nfs_shares() {
    apt-get -y install nfs-common
    
    mkdir -p /root/spark/data
    
    grep "$1:/root/spark/data" /etc/fstab
    if [ $? -ne 0 ]; then
        echo "$1:/root/spark/data /root/spark/data  nfs     nfsvers=3,rw,async    0   0" >> /etc/fstab
    fi

    mount -a
    
    # Ensure shared directory has been mounted.
    mount | grep /root/spark/data
}

# Start system CPU and memory usage collection using dstat.
#  $1 -> Output metrics to this directory
start_system_metrics() {
    local report_dir="$1"

    if [ -f "/root/.dstat_pid" ]; then
        echo "Error: Reporting is already started. Stop it first using stop-metrics or kill dstat process and delete /root/.dstat_pid"
        return 1
    fi
    
    # Since dstat appends a bunch of headers and newlines on every call by default, the CSV file becomes
    # difficult to process. So prevent user from collecting to an existing file.
    if [ -d "$report_dir" ]; then
        echo "Error: Report directory already exists. Provide a different directory."
        return 1
    fi
    
    mkdir -p "$report_dir"
    
    # Find number of processors.
    local num_cpus=$(cat /proc/cpuinfo | grep '^processor' | wc -l)
    local cpu_ids="$(seq -s ',' 0 $((num_cpus - 1)))"
    # dstat output columns are:
    #--epoch--- -------cpu0-usage--------------cpu1-usage--------------cpu2-usage--------------cpu3-usage------- ------memory-usage-----
    #   epoch   |usr sys idl wai hiq siq:usr sys idl wai hiq siq:usr sys idl wai hiq siq:usr sys idl wai hiq siq| used  buff  cach  free
    nohup dstat -T -c -C "$cpu_ids" -m --noheaders --output "$report_dir/dstat.csv" > /dev/null 2>&1 &
    local dstat_pid=$!
    echo "$dstat_pid" > "/root/.dstat_pid"
    
    # Collect disk free metrics. This is because Spark consumes 10s of GBs of /tmp for shuffle operations.
    nohup ./slave.sh collect-df "$report_dir/df.csv" 5 > /dev/null 2>&1  &
    local df_pid=$!
    echo "$df_pid" > "/root/.df_pid"
    
    echo "Started CPU, RAM, disk space collection to $report_dir"
    
    return 0
}

stop_system_metrics() {
    if [ -f "/root/.dstat_pid" ]; then
    
        kill -9 "$(cat /root/.dstat_pid)"
        if [ $? -eq 0 ]; then
            echo "Stopped dstat metrics collection"
            rm -f "/root/.dstat_pid"
        else
            echo "Unable to stop dstat metrics collection. Kill PID $(cat /root/.dstat_pid) manually."
        fi
    else
        echo "Error: Does not look like dstat is running"
    fi

    if [ -f "/root/.df_pid" ]; then
    
        kill -9 "$(cat /root/.df_pid)"
        if [ $? -eq 0 ]; then
            echo "Stopped df metrics collection"
            rm -f "/root/.df_pid"
        else
            echo "Unable to stop df metrics collection. Kill PID $(cat /root/.df_pid) manually."
        fi
    else
        echo "Error: Does not look like df is running"
    fi
    
}



# Periodically collects disk free stats for /dev/root
# $1 -> Report file
# $2 -> Interval between collections
collect_df() {
    report_file=$1
    interval=$2

    while sleep "$interval"; do
        echo "$(date +%s) $(df -h | grep /dev/root)" | awk '{printf "%s,%s,%s,%s\n",$1,$3,$4,$5}' >> "$report_file"
    done
}







# For Spark to be able to use native linear algebra libraries like OpenBLAS or ATLAS,
# it requires some additional JARs that are not packaged with it. 
# This function installs them under SPARK_DIR/jars/
#
# $1 -> The Spark installation directory. It should have ./jars/ under it.
install_spark_native_stack() {
    local spark_dir="$1"
	if [ -z "$spark_dir" ]; then
		echo "Error: Missing Spark installation directory"
		return 1
	fi
    
    if [ ! -d "$spark_dir/jars" ]; then
        echo "Error: $spark_dir does not seem to be a Spark installation"
		return 1
    fi
    
    # To integrate with native stacks, we need these additional JARS under SPARK_DIR/jars/
    # 1. com.github.fommil.netlib:native_system-java:1.1
    # 2. com.github.fommil.netlib:netlib-native_system-linux-x86_64:1.1
    # 3. com.github.fommil:jniloader:1.1
    wget -P "$spark_dir/jars/" \
        'http://repo1.maven.org/maven2/com/github/fommil/netlib/native_system-java/1.1/native_system-java-1.1.jar' \
        'http://repo1.maven.org/maven2/com/github/fommil/netlib/netlib-native_system-linux-x86_64/1.1/netlib-native_system-linux-x86_64-1.1-natives.jar' \
        'http://repo1.maven.org/maven2/com/github/fommil/jniloader/1.1/jniloader-1.1.jar'
}



case "$1" in

    # Prepare the system to run this script.
    init)
    init
    ;;
    
    install-slave)
    install_slave
    ;;
    
    install-prereqs)
    install_slave_node_prerequisites
    ;;
    
    install-spark)
    install_spark "$2"
    ;;
    
    install-spark-native)
    install_spark_native_stack "$2"
    ;;
    
    join-cluster)
    join_cluster "$2" "$3"
    ;;
    
    leave-cluster)
    leave_cluster "$2"
    ;;
    
    
    secure)
    secure 
    ;;
    
 
    start-metrics)
    start_system_metrics "$2"
    ;;
    
    stop-metrics)
    stop_system_metrics "$2"
    ;;

    collect-df)
    collect_df "$2" "$3"
    ;;

    setup-nfs)
    setup_nfs_shares "$2"
    ;;
    
    
    *)
    echo "Unknown command: $1"
    ;;
esac
