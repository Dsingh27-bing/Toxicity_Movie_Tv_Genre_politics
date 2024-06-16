import requests
import pprint
import time
import psycopg2
import random, sys
import csv
import pandas as pd
from io import StringIO

from datetime import datetime
import json

# Reddit API credentials
sys.path.append("../../")  # Add the parent directory to the sys.path

# Import configuration variables
from config import (
    client_id,
    client_secret,
    user_agent,
    username,
    password,
    moderate_api_key
)

if sys.path:
    sys.path.pop()
# Use the imported variables in your code


counter = 0
fetch_counter = 0



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


headers = {'User-Agent': 'YourApp/0.1'}

def fetch_comments(post_id):
    global counter

    token = get_reddit_token(client_id, client_secret, username, password)
    if token:
        auth_headers = {
            'User-Agent': 'Reddit_Project_API/0.0.1 by /u/CS515SMDP',
            'Authorization': f'Bearer {token}'
        }
        url = f"https://www.reddit.com/comments/{post_id}.json"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # print("got comments for" + str(post_id))
            # increment fetch counter
            global fetch_counter
            fetch_counter += 1
            print("got comments for" + str(post_id) + " " + str(fetch_counter))
            return response.json()
        else:
            print('putting to sleep')
            print(counter)
            counter += 1
            # time.sleep(60)
            return "rate_limit_exceeded"
    else:
        print("Failed to obtain Reddit token.")
        return '', None

def fetch_comments_from_link(link):
    global counter
    token = get_reddit_token(client_id, client_secret, username, password)
    if token:
        auth_headers = {
            'User-Agent': 'Reddit_Project_API/0.0.1 by /u/CS515SMDP',
            'Authorization': f'Bearer {token}'
        }
        response = requests.get(link + '.json', headers=headers)
        if response.status_code == 200:
            # print("got comments for" + str(post_id))
            # increment fetch counter
            global fetch_counter
            fetch_counter += 1
            print("got comments for" + str(link) + " " + str(fetch_counter))
            return response.json()
        else:
            print('putting to sleep')
            print(counter)
            counter += 1
            time.sleep(60)
            return link
    else:
        print("Failed to obtain Reddit token.")
        return '', None

    
   
def parse_comments(comments, post_id, parent_id=None):
    parsed_comments = []
    for comment in comments:
        if 'data' in comment:
            data = comment['data']
            if 'body' in data:  # Check if 'body' key exists
                parsed_comments.append({
                    'post_id': post_id,
                    'comment_id': data['id'],
                    'body': data['body'],
                    'score': data['score'],
                    'parent_id': parent_id
                })
                if 'replies' in data and data['replies']:  # Recursively parse replies
                    parsed_comments.extend(parse_comments(data['replies']['data']['children'], post_id, parent_id=data['id']))
    return parsed_comments
def get_toxicity_score(comment):
    api_url = "https://api.moderatehatespeech.com/api/v1/moderate/"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "token": moderate_api_key,  # Replace with your actual API token
        "text": comment
    }
    # set a global counter
    global cc
    cc += 1
    print("comment no :" + str(cc))
    try:
        response = requests.post(api_url, json=data, headers=headers)
        response.raise_for_status()  # This will raise an error for HTTP error codes

        # Check if response content is not empty
        if not response.content:
            print("empty response")
            # print("Received an empty response from the API.")
            return None

        try:
            result = response.json()
            if 'response' not in result:
                print("Response from API does not contain the 'response' key.")
                return 'error', 0
            if result['response'] == 'Success':
                return result['class'], result['confidence']
            else:
                print("Error response from API:", result)
                return 'error', 0
        except json.JSONDecodeError:
            print("Failed to decode JSON response.")
            print("Response content:", response.text)
            return 'error', 0

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return 'error', 0
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        return 'error', 0
        
def create_table_if_not_exists(cursor, table_name):
    query_create = f"CREATE TABLE IF NOT EXISTS {table_name} (\
                    POST_ID VARCHAR ,     \
                    COMMENT_ID VARCHAR PRIMARY KEY, \
                    BODY VARCHAR, \
                    TOXICITY VARCHAR,\
                    SCORE FLOAT\
                 );"
    cursor.execute(query_create)

def insert_data_into_postgresql(table_name, csv_file_path):
    try:
        with psycopg2.connect("dbname=crawler user=postgres") as connection:
            with connection.cursor() as cursor:
                create_table_if_not_exists(cursor, table_name)

                with open(csv_file_path, 'r') as file:
                    next(file)  # Skip header if present in the CSV file
                    csv_data = StringIO(file.read())
                    csv_reader = csv.reader(csv_data, delimiter=',')
                    
                    for row in csv_reader:
                        if len(row) != 5:
                            print(f"Skipping row due to incorrect number of columns: {row}")
                            continue

                        cursor.execute(
                            f'INSERT INTO {table_name} (POST_ID, COMMENT_ID, BODY, TOXICITY, SCORE) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (COMMENT_ID) DO NOTHING',
                            (row[0], row[1], row[2], row[3], row[4])
                        )

                connection.commit()
                print("Data inserted into PostgreSQL successfully!")
                
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to or interacting with PostgreSQL:", error)



def update_cache_file(last_line, cache_file_path):
    """Updates the cache file with the last processed line number."""
    with open(cache_file_path, 'w') as cache:
        cache.write(str(last_line))

def count_csv_lines(file_path):
    with open(file_path, 'r') as file:
        return sum(1 for row in file)


def main(csv_file, cache_file, results_file):
    # Initialize variables
    all_comments = []
    start_line = 0
    start_time = time.time()
    test = False
    cc=0
    total_lines = count_csv_lines(csv_file) - 1 # Count the total number of lines in the CSV
    print(f"Total number of lines in the CSV file: {total_lines}")

    # Clear the results_comments.csv file
    open(f'{results_file}.csv', 'w').close()


    # Read the last line number from the cache file
    try:
        with open(cache_file, 'r') as cache:
            last_line = cache.read().strip()
            if last_line: 
                start_line = int(last_line)
                print(f"Starting from line {start_line} in the CSV file.")
    except FileNotFoundError:
        print(f"Cache file {cache_file} not found. Starting from the beginning.")

    with open(csv_file, newline='') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader, start=1):
        # if we reach end of the csv file, break and set test to True        


            if i <= start_line:
                continue  # Skip processed lines
            # Check if this is the last line of the CSV




            post_id = row['post_id']  # Assuming 'post_id' is the columsn name
            comments_json = fetch_comments(post_id)
            cc +=1
            if (cc == total_lines):
                test = True
            if comments_json == "rate_limit_exceeded" or test==True:
                
                print("Processing the hate speech API requests...")
                # Create a Pandas DataFrame
                df = pd.DataFrame(all_comments)
            if 'body' in df.columns:
                try:
                    df['toxicity_result'] = df['body'].apply(get_toxicity_score)
                    df = df[df['toxicity_result'].notnull()]  # Remove comments with None results
                    df['toxicity_class'], df['confidence_score'] = zip(*df['toxicity_result'])
                    df.drop(columns=['toxicity_result'], inplace=True)  # Drop the temporary column
                # remove df['body'] column
                    df.drop(columns=['body'], inplace=True)  # Drop the temporary column
                    df.drop(columns=['parent_id'], inplace=True)  # Drop the temporary column
                except:
                    print("Error occurred while calculating toxicity score.")


            else:
                print("Warning: 'body' column not found in the DataFrame. Skipping toxicity score calculation.")
                df['toxicity_class'] = None
                df['confidence_score'] = None


            # remove df['body'] and df['parent_id'] columns if they exist
            for column in ['body', 'parent_id']:
                if column in df.columns:
                    df.drop(columns=[column], inplace=True)

            # drop df rows with toxicity_class = 'error'
            df = df[df['toxicity_class'] != 'error']
            # drop df with nonetype
            df = df[df['toxicity_class'].notnull()]
            df.to_csv(f"{results_file}.csv", index=False)
                
            # insert_data_into_postgresql('MOVIES_COMMENTS', 'results_comments.csv')
            print(f"Comment data has been saved to {results_file}.csv")
            update_cache_file(i, cache_file)
            print("it took " + str(time.time() - start_time) + " seconds to finish")
            return df
#             elif comments_json:
#                 comment_data = parse_comments(comments_json[1]['data']['children'], post_id)
#                 all_comments.extend(comment_data)

# if __name__ == "__main__":
    csv_file = 'movies_post_ids.csv'  # Replace with your CSV file path
    cache_file = 'cache_file.txt'
    csv_file2 = 'tv_post_ids.csv'  # Replace with your CSV file path
    cache_file2= 'cache_file_tv.txt'  # Replace with your CSV file path


# store the df from main function
    main(csv_file, cache_file, 'results_comments_movies')

    # # send the results_comments.csv to postgresql
    insert_data_into_postgresql('MOVIES_COMMENTS', 'results_comments_movies.csv')

    main(csv_file2, cache_file2, 'results_comments_tv')
    insert_data_into_postgresql('TV_COMMENTS', 'results_comments_tv.csv')
                
