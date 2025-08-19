#!/bin/bash
set -e

METASTORE_PORT=9083
METASTORE_MAX_ATTEMPTS=30
METASTORE_WAIT_SECONDS=5

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

wait_for_port() {
    local port=$1
    local max_attempts=$2
    local wait_seconds=$3
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        if netstat -tlnp 2>/dev/null | grep -q ":$port"; then
            return 0
        fi
        log "Attempt $attempt/$max_attempts - Port $port not ready..."
        attempt=$((attempt + 1))
        sleep "$wait_seconds"
    done
    return 1
}

wait_for_metastore_beeline() {
    local max_attempts=$1
    local wait_seconds=$2
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        if beeline -u "jdbc:hive2://" -e "SHOW DATABASES;" >/dev/null 2>&1; then
            return 0
        fi
        log "Attempt $attempt/$max_attempts - Hive Metastore not responding via Beeline..."
        attempt=$((attempt + 1))
        sleep "$wait_seconds"
    done
    return 1
}

if [ $attempt -lt $max_attempts ]; then
    log "Starting Hive Metastore..."
    hive --service metastore > "$HADOOP_HOME/logs/metastore.log" 2>&1 &

    log "Waiting for Hive Metastore port $METASTORE_PORT..."
    if ! wait_for_port $METASTORE_PORT $METASTORE_MAX_ATTEMPTS $METASTORE_WAIT_SECONDS; then
        log "ERROR: Hive Metastore did not start!"
        cat "$HADOOP_HOME/logs/metastore.log" || true
        exit 1
    fi

    if [ -f "$HADOOP_HOME/logs/metastore.log" ]; then
        log "Hive Metastore initialized successfully!"
    else
        log "Hive Metastore failed to initialize!"
        cat "$HADOOP_HOME/logs/metastore.log" || true
        exit 1
    fi

    log "Configuring Hive Metastore..."
    schematool -dbType postgres -info || schematool -dbType postgres -initSchema
    schematool -dbType postgres -validate || schematool -dbType postgres -upgradeSchema
fi

log "Waiting for Hive Metastore to respond via Beeline..."
if ! wait_for_metastore_beeline $METASTORE_MAX_ATTEMPTS $METASTORE_WAIT_SECONDS; then
    log "ERROR: Hive Metastore is not responding!"
    exit 1
fi

log "Creating bronze schema in metastore..."
beeline -u "jdbc:hive2://" -e "CREATE SCHEMA IF NOT EXISTS BRONZE;"
