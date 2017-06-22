#!/bin/bash

SPARK_VERSION='spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz'

# Prepare the system to run this script.
init() {
    apt-get -y update
    apt-get -y install jq curl wget tar bc
    
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
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
    
    apt-get -y update
    apt-get -y install tmux openjdk-8-jre-headless dstat python3 python3-pip git sbt
    
    # Create Python environment for recommender app.
    pip3 install virtualenv
    
    if [ ! -d "/root/spark/recommender/app/pyenv" ]; then
        virtualenv -p python3 /root/spark/recommender/app/pyenv
        source /root/spark/recommender/app/pyenv/bin/activate
        pip install google-api-python-client beautifulsoup4 feedparser PyYAML requests
        deactivate
    fi
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
    
    cp "/root/spark/recommender/deploy/spark-defaults.conf" "$installed_dir/conf/"
    cp "/root/spark/recommender/deploy/metrics.properties" "$installed_dir/conf/"
    
    echo "Spark installed in: $installed_dir"
}

install_recommender_app() {
    # TODO
    git clone https://github.com/pathbreak/content-recommender-spark-lda /root/spark/recommender
    
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
    # Runs the LDA spark app in local execution mode on the master node.
    # The important settings are:
    #   --driver-memory MEM : Sets maximum heap space -Xmx to MEM
    #   --conf spark.driver.maxResultSize=SIZE: Some of the results like collect/take result in massive
    #           results that exceed the default 1G size.
    local system_ram_mb=$(grep MemTotal /proc/meminfo | awk '{print $2}' | xargs -I {} echo "{}/1024" | bc)
    
    # Set driver max heap space to system_ram_mb - 512 MB
    local driver_max_heap_mb=$(echo "$system_ram_mb - 512" | bc)
    local max_result_size_mb=$((driver_max_heap_mb / 2))
    
    local spark_dir="$1"
    "$spark_dir/bin/spark-submit" --driver-memory "$driver_max_heap_mb"M \
        --conf spark.driver.maxResultSize="$max_result_size_mb"M \
        /root/spark/lda-prototype.jar \
        "$2" "$3" "$4" "$5"
}


# Start system CPU and memory usage collection using dstat.
#  $1 -> Output to this file
start_system_metrics() {
    local report_file="$1"

    if [ -f "/root/.dstat_pid" ]; then
        echo "Error: Reporting is already started. Stop it first."
        return 1
    fi
    
    # Since dstat appends a bunch of headers and newlines on every call by default, the CSV file becomes
    # difficult to process. So prevent user from collecting to an existing file.
    if [ -f "$report_file" ]; then
        echo "Error: Report file already exists. Provide a different filename."
        return 1
    fi
    
    # Find number of processors.
    local num_cpus=$(cat /proc/cpuinfo | grep '^processor' | wc -l)
    local cpu_ids="$(seq -s ',' 0 $((num_cpus - 1)))"
    # dstat output columns are:
    #--epoch--- -------cpu0-usage--------------cpu1-usage--------------cpu2-usage--------------cpu3-usage------- ------memory-usage-----
    #   epoch   |usr sys idl wai hiq siq:usr sys idl wai hiq siq:usr sys idl wai hiq siq:usr sys idl wai hiq siq| used  buff  cach  free
    nohup dstat -T -c -C "$cpu_ids" -m --noheaders --output "$report_file" > /dev/null 2>&1 &
    local dstat_pid=$!
    echo "$dstat_pid" > "/root/.dstat_pid"
    
    return 0
}

stop_system_metrics() {
    if [ ! -f "/root/.dstat_pid" ]; then
        echo "Error: Does not look like dstat is running"
        return 1
    fi
    kill -9 "$(cat /root/.dstat_pid)"
    rm -f "/root/.dstat_pid"
}



enable_nfs_sharing() {
    apt-get -y install nfs-kernel-server
    
    systemctl start nfs-kernel-server.service
}

disable_nfs_sharing() {
    systemctl stop nfs-kernel-server.service
}

# Add a Spark slave as permitted NFS client.
#   $1 => The private IP address of client in CIDR notation. Example: 192.168.11.239/17
add_nfs_client() {
    # /etc/exports allows the same directory to be repeated on multiple lines for different clients.
    # This makes grepping and adding or replacing much easier compared to having all clients on a 
    # single line.
    local worker_ip="$1"
    echo "/root/spark/data    $1(rw,sync,no_subtree_check,no_root_squash)" > /etc/exports
    exportfs -a
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
    
    install-spark)
    install_spark "$2"
    ;;
    
    install-spark-native)
    install_spark_native_stack "$2"
    ;;
    
    
    run-local)
    run_lda_local "${@:2}"
    ;;
    
    start-metrics)
    start_system_metrics "$1"
    ;;
    
    stop-metrics)
    stop_system_metrics "$1"
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
