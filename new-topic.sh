if [ -z "$1" ]; then
    echo "Usage: $0 <name>"
    exit 1
fi
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic "$1"
