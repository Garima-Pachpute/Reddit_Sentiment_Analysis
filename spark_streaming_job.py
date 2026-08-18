import os
import re
import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StructType, StringType
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

# NLTK Setup
nltk.download('stopwords')
nltk.download('wordnet')

stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def clean_text(text):
    text = re.sub(r"http\S+|www\S+|https\S+", '', str(text))
    text = re.sub(r'[^A-Za-z\s]', '', text.lower())
    tokens = text.split()
    tokens = [lemmatizer.lemmatize(w) for w in tokens if w not in stop_words]
    return " ".join(tokens)

def get_sentiment(text):
    pos_words = ['good', 'great', 'love', 'excellent', 'happy']
    neg_words = ['bad', 'worst', 'hate', 'awful', 'sad']
    score = sum(1 for w in text.split() if w in pos_words) - sum(1 for w in text.split() if w in neg_words)
    return "positive" if score > 0 else "negative" if score < 0 else "neutral"

# Secure credential retrieval using environment variables
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "YOUR_AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "YOUR_AWS_SECRET_ACCESS_KEY")

# Spark session with S3 support
spark = SparkSession.builder \
    .appName("RedditSentimentS3") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType().add("text", StringType())

# Register UDFs
clean_udf = udf(clean_text, StringType())
sentiment_udf = udf(get_sentiment, StringType())

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_topic") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse, clean, score
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
cleaned_df = parsed_df.withColumn("clean_text", clean_udf(col("text")))
scored_df = cleaned_df.withColumn("sentiment", sentiment_udf(col("clean_text")))

# Save to Amazon S3 (Fixed missing slash in checkpoint path)
query = scored_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://reddit-streaming-data/reddit_output/") \
    .option("checkpointLocation", "s3a://reddit-streaming-data/reddit_checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()