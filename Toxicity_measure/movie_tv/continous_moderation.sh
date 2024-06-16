#!/bin/bash
while true; do
    current_time=$(date +'%H:%M')
    echo "Running the script at $current_time..."
    rm moderation_comments_*.csv
    rm tmdb_data_*.csv
    source /home/rkale2/myenv/bin/activate
    cd /home/rkale2/project2/moderation
    python3 moderation.py
    deactivate
    psql -U postgres <<EOF
    \c crawler
    UPDATE reddit_movies AS rm
SET
    flag_count = COALESCE(subquery.flag_count, 0),
    normal_count = COALESCE(subquery.normal_count, 0)
FROM (
    SELECT
        post_id,
        COUNT(CASE WHEN toxicity = 'flag' THEN 1 END) AS flag_count,
        COUNT(CASE WHEN toxicity = 'normal' THEN 1 END) AS normal_count
    FROM
        movies_comments
    GROUP BY
        post_id
) AS subquery
WHERE rm.post_id = subquery.post_id;

UPDATE reddit_tv AS rm
SET
    flag_count = COALESCE(subquery.flag_count, 0),
    normal_count = COALESCE(subquery.normal_count, 0)
FROM (
    SELECT
        post_id,
        COUNT(CASE WHEN toxicity = 'flag' THEN 1 END) AS flag_count,
        COUNT(CASE WHEN toxicity = 'normal' THEN 1 END) AS normal_count
    FROM
        tv_comments
    GROUP BY
        post_id
) AS subquery
WHERE rm.post_id = subquery.post_id;

UPDATE tmdb_movies_new AS tb                                                      
SET
    flag = COALESCE(subquery.flag_count, 0),
    normal = COALESCE(subquery.normal_count, 0)
FROM (
    SELECT
        name,
        SUM(flag_count) AS flag_count,
        SUM(normal_count) AS normal_count
    FROM
        reddit_movies
    GROUP BY
        name
) AS subquery
WHERE tb.title = subquery.name;


UPDATE tmdb_tv_new AS tb                                                      
SET
    flag = COALESCE(subquery.flag_count, 0),
    normal = COALESCE(subquery.normal_count, 0)
FROM (
    SELECT
        name,
        SUM(flag_count) AS flag_count,
        SUM(normal_count) AS normal_count
    FROM
        reddit_tv
    GROUP BY
        name
) AS subquery
WHERE tb.title = subquery.name;

\COPY (SELECT genre, SUM(normal) AS total_normal, SUM(flag) AS total_flag FROM (SELECT unnest(string_to_array(regexp_replace(genres::text, '[{}" ]', '', 'g'), ',')) AS genre, normal, flag FROM tmdb_movies_new) AS unnested_data GROUP BY genre ORDER BY genre) TO '/home/rkale2/project2/moderation/genre_report_movies.csv' WITH CSV HEADER;

\COPY (SELECT genre, SUM(normal) AS total_normal, SUM(flag) AS total_flag FROM (SELECT unnest(string_to_array(regexp_replace(genres::text, '[{}" ]', '', 'g'), ',')) AS genre, normal, flag FROM tmdb_tv_new) AS unnested_data GROUP BY genre ORDER BY genre) TO '/home/rkale2/project2/moderation/genre_report_tv.csv' WITH CSV HEADER;

    select count(*) from movies_comments;
    select count(*) from tv_comments;
    select count(*) from (select count(*) from movies_comments group by post_id);
    \copy tmdb_movies_new TO '/home/rkale2/project2/moderation/tmdb_data_movies.csv' WITH CSV HEADER
    \copy tmdb_tv_new TO '/home/rkale2/project2/moderation/tmdb_data_tv.csv' WITH CSV HEADER

    \q
EOF
    # psql -U postgres -d crawler -c "\copy movies_comments TO '/home/rkale2/project2/moderation/moderation_comments.csv' WITH CSV HEADER"
    # psql -U postgres -d crawler -c "\copy tv_comments TO '/home/rkale2/project2/moderation/moderation_comments_tv.csv' WITH CSV HEADER"
    echo "trying again in 2 minutes..."
    echo -e "\n"
    echo -e "\n"
    sleep 300 # Sleep for 2 minutes
done


