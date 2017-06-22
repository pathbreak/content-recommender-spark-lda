#!/bin/bash

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
    
    cp "/root/spark/recommender/deploy/spark-defaults.conf" "$installed_dir/conf/"
    cp "/root/spark/recommender/deploy/metrics.properties" "$installed_dir/conf/"
    
    echo "Spark installed in: $installed_dir"
}


# Mount the spark master's NFS shared data directory locally.
#   $1 -> Spark Master's private IP address
setup_nfs_shares() {
    apt-get -y install nfs-common
    
    mkdir -p /root/spark/data
    echo "$1:/root/spark/data /root/spark/data  nfs     nfsvers=3,rw,async    0   0" >> /etc/fstab
    mount -a
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

