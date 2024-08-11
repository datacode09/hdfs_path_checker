#!/bin/bash

# Define variables
HDFS_PATHS=(
    "hdfs://prod-ns:8020/prod/01559/app/RIEQ/data.ide/ATOMDataFiles/Productivity/CAG5BR/Outputs/"
    # Add more paths here as needed
)
LOGS_DIR="./logs"
REPORT_FILE="$(pwd)/hdfs_report_last_10_days.html"  # Save the report in the current directory
LOG_FILE="${LOGS_DIR}/$(date +%Y-%m-%d_%H-%M-%S).log"

# Create directories if they don't exist
mkdir -p "${LOGS_DIR}"

# Log function
log() {
    echo "$(date +%Y-%m-%d_%H-%M-%S) - $1" | tee -a "${LOG_FILE}"
}

# Function to check for files modified between 12:00 AM and 6:00 AM for a specific date
check_files_in_directory_for_date() {
    local hdfs_path=$1
    local date=$2
    log "Checking files in directory: ${hdfs_path} for date: ${date}"

    local cmd="hdfs dfs -ls ${hdfs_path} | awk -v target_date=${date} '\$6 == target_date && (\$7 >= \"00:00\" && \$7 < \"06:00\") {print \$8}'"
    eval "${cmd}" > /tmp/modified_files.txt

    if [ -s /tmp/modified_files.txt ]; then
        while IFS= read -r file; do
            echo "<tr><td>${date}</td><td>${hdfs_path}</td><td>${file}</td></tr>" >> "${REPORT_FILE}"
        done < /tmp/modified_files.txt
        log "Modified files found in ${hdfs_path} for ${date}."
    else
        echo "<tr><td>${date}</td><td>${hdfs_path}</td><td>No files modified</td></tr>" >> "${REPORT_FILE}"
        log "No modified files found in ${hdfs_path} for ${date}."
    fi
}

# Main function
main() {
    log "Starting main function."

    # Start the HTML report
    echo "<html><body><table border=\"1\">" > "${REPORT_FILE}"
    echo "<tr><th>Date</th><th>HDFS Path</th><th>File</th></tr>" >> "${REPORT_FILE}"

    for i in {0..9}; do
        # Calculate the date for i days ago
        date=$(date -d "-${i} day" +%Y-%m-%d)

        for hdfs_path in "${HDFS_PATHS[@]}"; do
            check_files_in_directory_for_date "${hdfs_path}" "${date}"
        done
    done

    # Close the HTML report
    echo "</table></body></html>" >> "${REPORT_FILE}"

    log "HTML report for the last 10 days saved to ${REPORT_FILE}"
    log "Ending main function."
}

# Run the main function
main
