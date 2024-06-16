import requests
import psycopg2
import sys
import datetime

sys.stdout = open('/dev/null', 'w')

# Reddit API credentials
client_id = "v-L_Un0PpdtiVZ6Eu30Sxw"
client_secret = "MEJIdGSeeqVdJlZcyBvi60W9CTcCuA"
user_agent = "Reddit_Project_API/0.0.1 by /u/CS515SMDP"
username = "CS515SMDP"
password = "DDJJR@2108"


# %%


def get_reddit_token(client_id, client_secret, username, password):
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "password", "username": username, "password": password}
    headers = {"User-Agent": user_agent}

    res = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
    )

    if res.status_code == 200:
        token = res.json().get("access_token")
        return token
    else:
        print("Failed to authenticate. Status code:", res.status_code)
        print("Error response:", res.json())
        return None


def reddit_comment_scraper(subreddit, max_requests):
    token = get_reddit_token(client_id, client_secret, username, password)
    politics_comments = []
    today = datetime.datetime.now()
    tomorrow = (today + datetime.timedelta(days=1))
    after = datetime.datetime(today.year, today.month, today.day)
    before = datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day)
    page_count = 0

    if token:
        headers = {"Authorization": "bearer " + token, "User-Agent": user_agent}
        params = {
            "limit": 100,  # Max limit is 100, limit is the number of entries per page
            # "after": after,
            # "before":before,
        }

        print("Starting to scrape comments at:", today.strftime("%Y-%m-%d %H:%M:%S"))
        requests_made = 0
        while requests_made < max_requests:
            if after:
                params["after"] = after
            url = f"https://oauth.reddit.com/r/{subreddit}/comments/.json"
            res = requests.get(url, headers=headers, params=params)

            if res.status_code == 200:
                page_count += 1
                print("Successfully retrieved data")
                print("Requests made:", requests_made)
                print("Page count:", page_count)
                top_comments = res.json()
                politics_comments.extend(top_comments["data"]["children"])
                if top_comments["data"]["children"]:
                    after = top_comments["data"]["children"][-1]["data"]["name"]
                else:
                    print("No more comments to scrape.")
                    break
                if after is None:
                    break
            else:
                print("Failed to retrieve data. Status code:", res.status_code)
                break

            requests_made += 1

        time_elapsed = datetime.datetime.now() - today

        print("Finished scraping comments at:", datetime.datetime.now())
        print("Time elapsed:", str(time_elapsed).split(".")[0])
        print("Total comments scraped:", len(politics_comments))
        return politics_comments


def connect_to_postgresql():
    try:
        connection = psycopg2.connect("dbname=comments_crawler user=postgres")
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def insert_data_into_postgresql(connection, table_name, dataset):
    if connection is not None:
        try:
            cursor = connection.cursor()
            if table_name == "NEW_COMMENTS":
                query_create = f"CREATE TABLE IF NOT EXISTS {table_name} (\
                    COMMENT_ID VARCHAR PRIMARY KEY,     \
                    COMMENT_DATE DATE, \
                    COMMENT_TIME TIME \
                );"
                cursor.execute(query_create)
                for key,value in dataset.items():
                    cursor.execute(
                        "INSERT INTO {} (COMMENT_ID, COMMENT_DATE, COMMENT_TIME) VALUES (%s, %s, %s) ON CONFLICT (COMMENT_ID) DO NOTHING".format(
                            table_name
                        ),
                        (key,value[0], value[1]),
                    )

            connection.commit()
            print("Data inserted into PostgreSQL successfully !!!!!!!")

        except (Exception, psycopg2.Error) as error:
            connection.rollback()
            print("Error while inserting data into PostgreSQL:", error)
        finally:
            cursor.close()
    else:
        print("Connection to PostgreSQL failed. Data not inserted.")

def main():
    subreddit_name = 'politics'
    comments = reddit_comment_scraper(subreddit_name,100)
    dataset={}

    # Process and print the comments
    print(len(comments))
    for comment in comments:
        comment_id = comment["data"]["id"]
        comment_created = datetime.datetime.fromtimestamp(comment["data"]["created"])
        comment_created_date = comment_created.date()
        comment_created_time = comment_created.time()
        dataset[comment_id] = [comment_created_date, comment_created_time]

    connection = connect_to_postgresql()

    if connection is not None:
        insert_data_into_postgresql(connection, "NEW_COMMENTS", dataset)

    # Close the PostgreSQL connection
    if connection is not None:
        connection.close()
        print("PostgreSQL connection is closed")

if __name__ == '__main__':
    main()
