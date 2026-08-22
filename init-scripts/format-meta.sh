#!/bin/bash
# Generate meta.properties for KRaft mode before Kafka starts
# Kafka requires meta.properties in each broker's data directory

NODE_ID="${KAFKA_CFG_NODE_ID:-1}"
CLUSTER_ID="${KAFKA_KRAFT_CLUSTER_ID:-}"

# Only format if not already formatted
if [ ! -f "/bitnami/kafka/data/meta.properties" ]; then
    echo "version=1" > /bitnami/kafka/data/meta.properties
    echo "node.id=${NODE_ID}" >> /bitnami/kafka/data/meta.properties
    if [ -n "$CLUSTER_ID" ]; then
        echo "cluster.id=${CLUSTER_ID}" >> /bitnami/kafka/data/meta.properties
    fi
    echo "Formatted meta.properties: node=${NODE_ID}, cluster=${CLUSTER_ID}"
else
    echo "meta.properties already exists, skipping format"
fi

# Execute the original entrypoint
exec /opt/bitnami/scripts/kafka/entrypoint.sh /opt/bitnami/scripts/kafka/run.sh