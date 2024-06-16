import requests
from datetime import datetime
import psycopg2,sys


sys.stdout = open('/dev/null', 'w')
sys.stderr = open('/dev/null', 'w')

# Reddit API credentials
client_id = 'demo'
client_secret = 'demo'
user_agent = "demo"
username = 'demo'
password = 'Demo'

def get_reddit_token(client_id, client_secret, username, password):
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }
    headers = {'User-Agent': user_agent}

    res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)

    if res.status_code == 200:
        token = res.json().get('access_token')
        return token
    else:
        print("Failed to authenticate. Status code:", res.status_code)
        print("Error response:", res.json())
        return None

token = get_reddit_token(client_id, client_secret, username, password)

if token:
    headers = {
        'User-Agent': user_agent,
        'Authorization': f'Bearer {token}'
    }

    # Get the current date in YYYY-MM-DD format
    current_date = datetime.now().strftime('%Y-%m-%d')
    # current_date = "2023-11-01"

    # Define the subreddit and parameters to fetch new posts for today
    subreddit = 'politics'
    params = {
        'limit': 100,  # Maximum limit per request
        'sort': 'new',  # Sort by new posts
    }

    file_name = "reddit_api_script.txt"
    post_data = []  # Initialize the list to store all posts
    post_count = 0

    after = None  # Initialize 'after' parameter
    requests_made = 0

    while True:
        if after:
            params['after'] = after  # Set the 'after' parameter to the last post ID

        response = requests.get(f'https://oauth.reddit.com/r/{subreddit}/new', headers=headers, params=params)

        if response.status_code != 200:
            print("Failed to fetch new posts. Status code:", response.status_code)
            print("Error response:", response.json())
            break

        new_posts = response.json()['data']['children']

        if not new_posts:
            break

        for post in new_posts:
            post_date = datetime.utcfromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%d')
            post_time = datetime.utcfromtimestamp(post['data']['created_utc']).strftime('%H:%M:%S')

            # Check if the post was submitted today
            if post_date == current_date:
                post_id = post['data']['id']
                post_title = post['data']['title']
                post_dict = {
                    "id": post_id,
                    "title": post_title,
                    "post_date" :post_date,
                    "post_time" : post_time,
                }
                post_data.append(post_dict)
                post_count += 1

                with open(file_name, "a") as file:
                    file.write(f"Post ID: {post_id}\n")
                    file.write(f"Post Title: {post_title}\n")
                    file.write(f"Score: {post['data']['score']}\n")
                    file.write("\n")
                
                

        # Update the 'after' parameter for the next request
        after = new_posts[-1]['data']['name']
        requests_made += 1

    # Print the post data dictionary
    for post_dict in post_data:
        print(post_dict)

    print("\n POST COUNT:", post_count)
    print("\n DATE: ", current_date)
    print("Requests made:", requests_made)

else:
    print("Failed to obtain access token.")


def connect_to_postgresql():
    try:
        connection = psycopg2.connect("dbname=newpost_crawler user=postgres")
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def insert_data_into_postgresql(connection, table_name,data_to_insert):
    if connection is not None:
        try:
            cursor = connection.cursor()
            if table_name == 'POSTS_CRAWLER':
                query_create = f"CREATE TABLE IF NOT EXISTS {table_name} (\
                    POST_ID VARCHAR PRIMARY KEY,     \
                    TITLE VARCHAR, \
                    POST_DATE DATE, \
                    POST_TIME TIME \
                );"
                cursor.execute(query_create)
                for data in data_to_insert:
                    # print(f"{data['id']}    {data['post_date']} \t {data['post_time']}\n")
                    cursor.execute(
                        'INSERT INTO {} (POST_ID, TITLE, POST_DATE,POST_TIME) VALUES (%s, %s,%s, %s) ON CONFLICT (POST_ID) DO NOTHING'.format(table_name),
                        (data['id'], data['title'],data['post_date'],data['post_time'])
                    )            
            connection.commit()
            # print("Data inserted into PostgreSQL successfully !")
            
        except (Exception, psycopg2.Error) as error:
            connection.rollback()
            print("Error while inserting data into PostgreSQL:", error)
        finally:
            cursor.close()
    else:
        print("Connection to PostgreSQL failed. Data not inserted.")


connection = connect_to_postgresql()

if connection is not None:
    insert_data_into_postgresql(connection, "POSTS_CRAWLER", post_data)

# Close the PostgreSQL connection
if connection is not None:
    connection.close()
    print("PostgreSQL connection is closed")
