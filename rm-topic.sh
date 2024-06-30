if [ -z "$1" ]; then
    echo "Usage: $0 <name>"
    exit 1
fi
kafka-topics --bootstrap-server localhost:9092 --delete --topic "$1"