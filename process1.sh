#!/bin/bash

# Define variables
HDFS_PATHS=(
    "hdfs://prod-ns:8020/prod/01559/app/RIEQ/data.ide/ATOMDataFiles/Productivity/CAG5BR/Outputs/"
    # Add more paths here as needed
)
LOGS_DIR="./logs"
LOG_FILE="${LOGS_DIR}/$(date +%Y-%m-%d_%H-%M-%S).log"
REPORT_FILE="$(pwd)/hdfs_report.html"  # Save the report in the current directory

# Create logs directory if it doesn't exist
mkdir -p "${LOGS_DIR}"

# Log function
log() {
    echo "$(date +%Y-%m-%d_%H-%M-%S) - $1" | tee -a "${LOG_FILE}"
}

# Function to check for files modified between 12:00 AM and 6:00 AM
check_files_in_directory() {
    local hdfs_path=$1
    log "Checking files in directory: ${hdfs_path}"

    local cmd="hdfs dfs -ls ${hdfs_path} | awk -v today=\$(date +%Y-%m-%d) '\$6 == today && (\$7 >= \"00:00\" && \$7 < \"06:00\") {print \$8}'"
    eval "${cmd}" > /tmp/modified_files.txt

    if [ -s /tmp/modified_files.txt ]; then
        echo "<h3>${hdfs_path}</h3>" >> "${REPORT_FILE}"
        echo "<ul>" >> "${REPORT_FILE}"
        while IFS= read -r file; do
            echo "<li>${file}</li>" >> "${REPORT_FILE}"
        done < /tmp/modified_files.txt
        echo "</ul>" >> "${REPORT_FILE}"
        log "Modified files found in ${hdfs_path}."
    else
        log "No modified files found in ${hdfs_path}."
    fi
}

# Main function
main() {
    log "Starting main function."

    # Start the HTML report
    echo "<html><body>" > "${REPORT_FILE}"

    for hdfs_path in "${HDFS_PATHS[@]}"; do
        check_files_in_directory "${hdfs_path}"
    done

    # Close the HTML report
    echo "</body></html>" >> "${REPORT_FILE}"

    log "HTML report saved to ${REPORT_FILE}"
    log "Ending main function."
}

# Run the main function
main
