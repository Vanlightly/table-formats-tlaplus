#!/bin/bash

# TODO yourself: 
# 1. Set the right location for Fizzbee
# 2. Choose the right number of iterations

# Array to store process IDs
pids=()

# Start 10 processes in the background (e.g., sleep for demo purposes)
for i in {1..10}; do
    ~/github/fizzbeeio/fizzbee/fizz --simulation iceberg.fizz | tee "iceberg-sim-$i.txt" &
    pids+=($!)
done

echo "Started 10 background processes."

# Function to check how many processes are still running
check_processes() {
    running_count=0
    for pid in "${pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            ((running_count++))
        fi
    done
    echo "$running_count processes are still running."
}

# Loop to periodically check running processes
while true; do
    check_processes
    if [ "$running_count" -eq 0 ]; then
        echo "All processes have completed."
        break
    fi
    sleep 2  # Wait for 2 seconds before checking again
done

