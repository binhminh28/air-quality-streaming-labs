#!/bin/bash

echo "🔄 Updating Cassandra schema..."

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Use first Cassandra node
CASSANDRA_NODE=$(docker ps --filter "name=cassandra" --format "{{.Names}}" | head -n 1)

if [ -z "$CASSANDRA_NODE" ]; then
    echo "❌ No Cassandra node found!"
    exit 1
fi

echo "Using node: $CASSANDRA_NODE"

# Check if keyspace exists
echo "🔍 Checking if keyspace exists..."
KEYSpace_EXISTS=$(docker exec "$CASSANDRA_NODE" cqlsh -e "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'air_quality';" 2>&1 | grep -c "air_quality" || echo "0")

if [ "$KEYSpace_EXISTS" -eq 0 ]; then
    echo "⚠️  Keyspace 'air_quality' does not exist. Running init script..."
    
    # Copy and run init script
    INIT_SCRIPT="$PROJECT_DIR/scripts/init_cassandra.cql"
    if [ -f "$INIT_SCRIPT" ]; then
        docker cp "$INIT_SCRIPT" "$CASSANDRA_NODE:/tmp/init_cassandra.cql"
        docker exec -i "$CASSANDRA_NODE" cqlsh -f /tmp/init_cassandra.cql
        if [ $? -eq 0 ]; then
            echo "✅ Keyspace and table created from init script"
        else
            echo "❌ Failed to create keyspace/table from init script"
            exit 1
        fi
    else
        echo "❌ Init script not found: $INIT_SCRIPT"
        exit 1
    fi
else
    echo "✅ Keyspace exists"
fi

# Update schema (add columns if they don't exist)
# Note: Cassandra doesn't support "ADD IF NOT EXISTS", so we ignore errors for existing columns
echo "📝 Adding columns if they don't exist..."
docker exec "$CASSANDRA_NODE" cqlsh <<EOF 2>&1 | grep -v "already exists" | grep -v "^$" || true
USE air_quality;
ALTER TABLE realtime_data ADD location_id BIGINT;
ALTER TABLE realtime_data ADD pm10 FLOAT;
ALTER TABLE realtime_data ADD pm1 FLOAT;
ALTER TABLE realtime_data ADD temperature FLOAT;
ALTER TABLE realtime_data ADD relativehumidity FLOAT;
ALTER TABLE realtime_data ADD um003 FLOAT;
ALTER TABLE realtime_data ADD aqi_pm25 INT;
ALTER TABLE realtime_data ADD aqi_pm10 INT;
EOF

# Show final schema
echo ""
echo "📊 Current schema:"
docker exec "$CASSANDRA_NODE" cqlsh -e "USE air_quality; DESCRIBE TABLE realtime_data;" 2>&1 | grep -A 30 "CREATE TABLE" | head -25

echo ""
echo "✅ Schema updated!"
