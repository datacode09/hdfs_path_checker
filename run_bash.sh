#!/bin/bash

# Define variables
HDFS_PATHS=(
    "hdfs://prod-ns:8020/prod/01559/app/RIEQ/data.ide/ATOMDataFiles/Productivity/CAG5BR/Outputs/"
    # Add more paths here as needed
)
LOGS_DIR="./logs"
LOG_FILE="${LOGS_DIR}/$(date +%Y-%m-%d_%H-%M-%S).log"
EMAIL_SUBJECT_SUCCESS="✔️ [Success] All HDFS Paths Modified"
EMAIL_SUBJECT_FAILURE="❌ [Failure] Some HDFS Paths Not Modified"
EMAIL_TO="recipient@example.com"
EMAIL_FROM="sender@example.com"
SMTP_SERVER="smtp.example.com"
SMTP_USER="user"
SMTP_PASS="password"

# Create logs directory if it doesn't exist
mkdir -p "${LOGS_DIR}"

# Log function
log() {
    echo "$(date +%Y-%m-%d_%H-%M-%S) - $1" | tee -a "${LOG_FILE}"
}

# Function to check for files modified between 12:00 AM and 6:00 AM
check_files_in_directory() {
    local hdfs_path=$1
    log "Starting function: check_files_in_directory for path ${hdfs_path}"

    local cmd="hdfs dfs -ls ${hdfs_path} | awk -v today=\$(date +%Y-%m-%d) '\$6 == today && (\$7 >= \"00:00\" && \$7 < \"06:00\")'"
    eval "${cmd}" > /tmp/modified_files.txt
    local modified_files_count=$(wc -l < /tmp/modified_files.txt)

    if [ "${modified_files_count}" -gt 0 ]; then
        log "Found ${modified_files_count} modified files in ${hdfs_path}"
        cat /tmp/modified_files.txt | tee -a "${LOG_FILE}"
        echo "Modified"
    else
        log "No files modified between 12:00 AM and 6:00 AM in ${hdfs_path}"
        echo "Not Modified"
    fi
}

# Function to generate HTML table
generate_html_table() {
    local data="$1"
    log "Starting function: generate_html_table"

    local html="<table border=\"1\">"
    html+="<tr><th>HDFS Path</th><th>Status</th><th>Modified Files</th></tr>"

    while IFS= read -r line; do
        local path=$(echo "$line" | cut -d '|' -f 1)
        local status=$(echo "$line" | cut -d '|' -f 2)
        local files=$(echo "$line" | cut -d '|' -f 3)

        html+="<tr><td>${path}</td><td>${status}</td><td><pre>${files}</pre></td></tr>"
    done <<< "$data"

    html+="</table>"
    echo "$html"
    log "Ending function: generate_html_table"
}

# Function to send email
send_email() {
    local subject="$1"
    local message="$2"
    log "Starting function: send_email"

    echo -e "Subject:${subject}\nFrom:${EMAIL_FROM}\nTo:${EMAIL_TO}\nContent-Type: text/html\n\n${message}" \
    | sendmail -t

    log "Email sent successfully."
    log "Ending function: send_email"
}

# Main function
main() {
    log "Starting function: main"

    local paths_status=""
    local all_paths_modified=true

    for hdfs_path in "${HDFS_PATHS[@]}"; do
        local status=$(check_files_in_directory "${hdfs_path}")
        local files=$(cat /tmp/modified_files.txt)

        if [ "${status}" == "Not Modified" ]; then
            all_paths_modified=false
        fi

        paths_status+="${hdfs_path}|${status}|${files}\n"
    done

    local table=$(generate_html_table "${paths_status}")

    if [ "${all_paths_modified}" == true ]; then
        send_email "${EMAIL_SUBJECT_SUCCESS}" "<p>All HDFS paths have been modified between 12:00 AM and 6:00 AM.</p>${table}"
    else
        send_email "${EMAIL_SUBJECT_FAILURE}" "<p>Some HDFS paths have not been modified between 12:00 AM and 6:00 AM.</p>${table}"
    fi

    log "Ending function: main"
}

# Run the main function
main
