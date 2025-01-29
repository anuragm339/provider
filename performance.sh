#!/bin/bash
#537
# Base URL and parameters
BASE_URL="http://localhost:8080/api/performance/parallel-test"
NUMBER_OF_CONSUMERS=15
MESSAGES_PER_CONSUMER=1000
INITIAL_MESSAGE_SIZE=524288  # Starting message size in bytes
MAX_MESSAGE_SIZE=10485760 # Maximum message size in bytes (e.g., 10 MB)

# Function to run the test
run_test() {
    local message_size=$1
    echo "Running test with message size: ${message_size} bytes"
    curl -X POST "${BASE_URL}?numberOfConsumers=${NUMBER_OF_CONSUMERS}&messagesPerConsumer=${MESSAGES_PER_CONSUMER}&messageSize=${message_size}"
    echo -e "\nTest completed for message size: ${message_size} bytes\n"
}

# Start interactive testing
message_size=$INITIAL_MESSAGE_SIZE
while [ $message_size -le $MAX_MESSAGE_SIZE ]; do
    run_test $message_size
    
    # Ask if the user wants to double the message size
    read -p "Do you want to double the message size to $((message_size * 2)) bytes? (y/n): " response
    case $response in
        [Yy]* ) message_size=$((message_size * 2));;
        [Nn]* ) echo "Performance testing stopped by user."; break;;
        * ) echo "Invalid input. Exiting."; break;;
    esac
done

echo "Performance testing completed."
