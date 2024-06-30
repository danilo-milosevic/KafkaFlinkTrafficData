if [ -z "$1" ]; then
    echo "Usage: $0 <name>"
    exit 1
fi

osascript -e "tell application \"Terminal\" to do script \"kafka-console-producer --broker-list localhost:9092 --topic $1\""

osascript -e "tell application \"Terminal\" to do script \"kafka-console-consumer --bootstrap-server localhost:9092 --topic $1 --from-beginning --partition 0\""
