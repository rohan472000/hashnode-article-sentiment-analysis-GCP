import requests

from google.cloud import bigquery
from google.oauth2 import service_account
from textblob import TextBlob


# Set up Google Cloud credentials
credentials = service_account.Credentials.from_service_account_file('service.json')


# Set up BigQuery client
client = bigquery.Client(credentials=credentials, project='clone-pho-076231')

# Set up Hashnode API endpoint
hashnode_api_endpoint = "https://api.hashnode.com"


# Define function to extract articles from Hashnode API
def extract_articles():
    # Set up Hashnode API endpoint
    hashnode_api_endpoint = "https://api.hashnode.com"
    # Define Hashnode API query
    query = '''
        query {
            user(username: "rohan-anand") {
                publication {
                    posts {
                        _id
                        title
                        slug
                        dateAdded
                    }
                }
            }
        }
    '''
    # Set up Hashnode API headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"   # your api-key
    }

    response = requests.post(hashnode_api_endpoint, 
                             headers=headers, json={'query': query})
    if response.status_code == 200:
        articles = response.json()['data']['user']['publication']['posts']
        return articles
    else:
        raise ValueError(f"Failed to retrieve articles: {response.content}")


# Define function to analyze articles using NLP
def analyze_articles(articles):
    analyzed_articles = []
    for article in articles:
        # Extract keywords from article content
        blob = TextBlob(article['title'])
        keywords = blob.noun_phrases
        # Extract sentiment from article content
        sentiment = blob.sentiment.polarity
        # Add keywords and sentiment to article dictionary
        article['keywords'] = keywords
        article['sentiment'] = sentiment
        article['date'] = article['dateAdded']
        analyzed_articles.append(article)
    return analyzed_articles


# Define function to insert data into BigQuery
def insert_data(analyzed_articles):
    # Define BigQuery table schema
    table_ref = client.dataset('earthquake').table('tableme')
    schema = [
        bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
        bigquery.SchemaField("sentiment", "FLOAT", mode="REQUIRED")
    ]
   
    # Create BigQuery table
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    table = client.create_table(table)

    # Insert data into BigQuery table
    rows_to_insert = []
    for article in analyzed_articles:
        rows_to_insert.append(
            (article['_id'], article['title'], article['dateAdded'], 
             article['keywords'], article['sentiment']))
    errors = client.insert_rows(table, rows_to_insert)
    if errors:
        print(f"Errors inserting rows into BigQuery: {errors}")
    else:
        print(f"Rows inserted into BigQuery: {len(rows_to_insert)}")


# Define main function
def main():
    articles = extract_articles()
    analyzed_articles = analyze_articles(articles)
    insert_data(analyzed_articles)


# Run main function
if __name__ == "__main__":
    main()
