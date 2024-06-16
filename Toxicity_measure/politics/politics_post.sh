#!/bin/bash

while true
do
    
  current_time=$(date +%Y-%m-%d %H:%M:%S)
  echo "Starting the script on $current_time"
  
  # Add your script commands here
  source myenv/bin/activate
  python3 politics_post_crawler.py
  deactivate
  echo -e "\n"
  echo -e "\n"
  psql -U postgres <<EOF
  \c newpost_crawler
  SELECT POST_DATE, COUNT(*) AS posts_count FROM posts_crawler GROUP BY post_date ORDER BY post_date;
  \q
EOF
  sleep 18000
done
