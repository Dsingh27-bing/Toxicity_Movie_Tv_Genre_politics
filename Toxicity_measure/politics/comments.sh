#!/bin/bash
while true; do
    current_time=$(date +'%H:%M')
    echo "Running the script at $current_time..."
    rm reddit_api_script.txt
    source /home/rkale2/myenv/bin/activate
    python3 politics_comment_crawler.py
    deactivate
    psql -U postgres <<EOF
    \c comments_crawler
    SELECT COMMENT_DATE, COUNT(*) AS comment_count FROM NEW_COMMENTS GROUP BY COMMENT_DATE ORDER BY COMMENT_DATE;
    \q
EOF
    echo "done"
    echo -e "\n"
    echo -e "\n"
    sleep 300  # Sleep for 5 minutes
done
