#!/bin/bash

SPARK_VERSION='spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz'

# Prepare the system to run this script.
init() {
    apt-get -y update
    apt-get -y install tmux jq curl wget tar bc
    
    mkdir -p /root/spark
    mkdir -p /root/spark/data
    mkdir -p /root/spark/data/historydata
    mkdir -p /root/spark/data/targetdata
    mkdir -p /root/spark/data/spark-events
    mkdir -p /root/spark/data/spark-csv
    
}

install_master() {
    install_master_node_prerequisites
    
    install_recommender_app
    
    install_spark "/root/spark/stockspark"
}

install_master_node_prerequisites() {
    # Install sbt repo
    echo "deb https://dl.bintray.com/sbt/debian /" | tee /etc/apt/sources.list.d/sbt.list
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
    
    apt-get -y update
    apt-get -y install openjdk-8-jre-headless dstat python3 python3-pip git
    
    # Create Python environment for recommender app.
    pip3 install google-api-python-client beautifulsoup4 feedparser PyYAML requests
    
    apt-get -y install sbt
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

install_recommender_app() {

    git clone https://github.com/pathbreak/content-recommender-spark-lda /root/spark/recommender
    
    chmod +x /root/spark/recommender/app/recommender_app.py
    
    sed -i 's|^HISTORY_DIR.*$|HISTORY_DIR: /root/spark/data/historydata|' /root/spark/recommender/app/conf/conf.yml
    sed -i 's|^TARGET_DIR.*$|TARGET_DIR: /root/spark/data/targetdata|' /root/spark/recommender/app/conf/conf.yml
    
    # Build the LDA spark driver JAR.
    cd /root/spark/recommender/spark
    sbt compile
    sbt assembly
    cp target/scala-2.11/lda-prototype.jar /root/spark/
}


# Runs the LDA job in local (ie, non-cluster) mode on the master itself.
#   $1 -> The directory where a spark installation exists to use for running this spark job.
#   $2 -> Training data directory (under /root/spark/data/historydata/)
#   $3 -> Targets data directory (under /root/spark/data/targetdata)
#   $4 -> Number of topics (k)
#   $5 -> Number of iterations
#   $6 -> Algorithm to use. "online"|"em"
#   $7 -> Path of a customs stop word list file
run_lda_local() {
    local spark_dir="$1"
    if [ ! -f "$spark_dir/bin/spark-submit" ]; then
        echo "Error: $spark_dir does not seem to be a Spark installation."
        return 1
    fi

    # Runs the LDA spark app in local execution mode on the master node.
    # The important settings are:
    #   --driver-memory MEM : Sets maximum heap space -Xmx to MEM
    #   --conf spark.driver.maxResultSize=SIZE: Some of the results like collect/take result in massive
    #           results that exceed the default 1G size.
    local system_ram_mb=$(grep MemTotal /proc/meminfo | awk '{print $2}' | xargs -I {} echo "{}/1024" | bc)
    
    # Set driver max heap space to 70% of system_ram_mb. For bc to give integer results,
    # the operation has to be a division.
    local driver_max_heap_mb=$(echo "scale=0;$system_ram_mb * 7/10" | bc)
    local max_result_size_mb=$(echo "scale=0;$driver_max_heap_mb * 1/2" | bc)
    
    local run_dir="/root/spark/data/sys-$(date +%Y-%m-%d-%H-%M-%S)"
    start_system_metrics "$run_dir"
    
    "$spark_dir/bin/spark-submit" --driver-memory "$driver_max_heap_mb"M \
        --conf spark.driver.maxResultSize="$max_result_size_mb"M \
        /root/spark/lda-prototype.jar \
        "$2" "$3" "$4" "$5" "$6" 2>&1 | tee -a "$run_dir/stdlogs"
    
    # Wait for sometime before stopping metrics collection, because memory and disk
    # cleanup take some time.
    sleep 15
    stop_system_metrics
}



# Starts the Spark master and a slave daemon on this machine's private IP address.
#   $1 -> The directory where a spark installation exists.
start_cluster() {
    local spark_dir="$1"
    if [ ! -f "$spark_dir/sbin/start-master.sh" ]; then
        echo "Error: $spark_dir does not seem to be a Spark installation."
        return 1
    fi
    
    # Since master script will requires non-interactive ssh access to slaves when job is started, 
    # we'll create a private key here.
    if [ ! -f /root/.ssh/id_rsa ]; then
        ssh-keygen -t rsa -b 4096 -N "" -f /root/.ssh/id_rsa 
    fi

    # Master daemon uses SPARK_LOCAL_IP only for port 8080 (WebUI), 
    # and --host for ports 6066 (REST endpoint) and 7077 (service)
    local private_ip=$(ip addr | grep 'eth0:1' | awk '{print $2}'|tr  '/' ' ' | awk '{print $1}')
    
    SPARK_LOCAL_IP=$private_ip  SPARK_PUBLIC_DNS=$private_ip  \
        "$spark_dir/sbin/start-master.sh" \
        "--host $private_ip"
    
    SPARK_LOCAL_IP=$private_ip SPARK_PUBLIC_DNS=$private_ip  \
        "$spark_dir/sbin/start-slave.sh" "--port 7078" \
        "--host $private_ip" "spark://$private_ip:7077"     
}

# Stops the Spark master and slave daemons on this machine.
#   $1 -> The directory where a spark installation exists.
stop_cluster() {
    local spark_dir="$1"
    if [ ! -f "$spark_dir/sbin/stop-master.sh" ]; then
        echo "Error: $spark_dir does not seem to be a Spark installation."
        return 1
    fi

    "$spark_dir/sbin/stop-slave.sh" 
    
    "$spark_dir/sbin/stop-master.sh"
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
    nohup ./master.sh collect-df "$report_dir/df.csv" 5 > /dev/null 2>&1  &
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




enable_nfs_sharing() {
    apt-get -y install nfs-kernel-server
    
    systemctl start nfs-kernel-server.service
}

disable_nfs_sharing() {
    systemctl stop nfs-kernel-server.service
}


# Add a Spark slave as permitted NFS client.
#   $1 => The private IP address of client. Example: 192.168.11.239
add_slave() {
    ssh-copy-id -i /root/.ssh/id_rsa "$1"
    
    add_nfs_client "$1"
}

# Add a Spark slave as permitted NFS client.
#   $1 => The private IP address of client.
add_nfs_client() {
    # /etc/exports allows the same directory to be repeated on multiple lines for different clients.
    # This makes grepping and adding or replacing much easier compared to having all clients on a 
    # single line.
    # The /17 subnet after slave's IP address is required.
    local worker_ip="$1"
    grep '/root/spark/data' /etc/exports | grep $worker_ip
    if [ $? -ne 0 ]; then
        echo "/root/spark/data    $worker_ip/17(rw,sync,no_subtree_check,no_root_squash)" > /etc/exports
        exportfs -a
    fi
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
    
    install-master)
    install_master
    ;;
    
    install-prereqs)
    install_master_node_prerequisites
    ;;
    
    install-spark)
    install_spark "$2"
    ;;
    
    install-spark-native)
    install_spark_native_stack "$2"
    ;;
    
    
    run-local)
    run_lda_local "${@:2}"
    ;;
    
    start-cluster)
    start_cluster "$2"
    ;;

    stop-cluster)
    stop_cluster "$2"
    ;;

    add-slave)
    add_slave "$2"
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

    enable-nfs)
    enable_nfs_sharing 
    ;;
    
    disable-nfs)
    disable_nfs_sharing
    ;;
    
    
    *)
    echo "Unknown command: $1"
    ;;

esac
