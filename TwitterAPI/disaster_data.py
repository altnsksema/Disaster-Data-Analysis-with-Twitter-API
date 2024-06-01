import requests
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from pyspark.sql import SparkSession

def fetch_tweets(keyword):
    url = "https://twitter154.p.rapidapi.com/search/search"
    querystring = {
        "query": keyword,
        "section": "top",
        "min_retweets": "1",
        "min_likes": "1",
        "limit": "2",
        "start_date": "2024-01-01",
        "language": "en"
    }
    headers = {
        "X-RapidAPI-Key": "e8568e3564msh1594a429e7a034bp1a2217jsn5d7a3c5279be",
        "X-RapidAPI-Host": "twitter154.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()

keywords = ["crash", "quarantine", "bush fires", "coronavirus"]
all_tweets = []

for keyword in keywords:
    tweets = fetch_tweets(keyword)
    all_tweets.extend(tweets['results'])
    time.sleep(1)  # Respect the API rate limit

print(json.dumps(all_tweets, indent=4))

# Step 2: Stream Tweets to Kafka
def create_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        return producer
    except NoBrokersAvailable:
        print("Error: No Kafka brokers available. Ensure that Kafka is running on localhost:9092.")
        return None

producer = create_kafka_producer()
if producer:
    def stream_to_kafka(tweets):
        for tweet in tweets:
            producer.send('disaster_tweets', tweet)

    stream_to_kafka(all_tweets)
else:
    print("Kafka producer could not be created. Exiting.")

# Step 3: Store Tweets in MongoDB
if producer:
    consumer = KafkaConsumer('disaster_tweets', bootstrap_servers='localhost:9092', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    client = MongoClient('localhost', 27017)
    db = client['disaster_data']
    collection = db['tweets']

    def consume_and_store():
        for message in consumer:
            try:
                tweet = message.value
                collection.insert_one(tweet)
            except Exception as e:
                 print("MongoDB'ye veri eklenirken bir hata oluştu:", e)
    consume_and_store()

# Step 4: Process Tweets with Spark
if producer:
    spark = SparkSession.builder.appName("DisasterTweetsAnalysis").config("spark.mongodb.input.uri", "mongodb://localhost:27017/disaster_data/tweets").getOrCreate()
    df = spark.read.format("mongo").load()
    df.createOrReplaceTempView("tweets")
    result = spark.sql("SELECT COUNT(*), keyword FROM tweets GROUP BY keyword")
    result.show()
