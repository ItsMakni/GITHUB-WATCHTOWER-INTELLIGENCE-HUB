import os
import re
import math
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField

# --- 1. UTILITY: SHANNON ENTROPY ---
def calculate_entropy(data):
    if not data:
        return 0
    entropy = 0
    # Filter for non-whitespace characters to find true "tokens"
    data = "".join(data.split())
    for x in range(256):
        p_x = float(data.count(chr(x))) / len(data)
        if p_x > 0:
            entropy += - p_x * math.log(p_x, 2)
    return entropy

# --- 2. MULTI-PURPOSE ANALYZER (Executors) ---
def analyze_partition(iterator, token):
    session = requests.Session()
    session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    })
    
    # Patterns for Tech Trends
    py_pattern = re.compile(r'^\+([a-zA-Z0-9\-_]+)', re.MULTILINE)
    js_pattern = re.compile(r'\+[\s]*"([^"]+)":')

    for row in iterator:
        repo_name = row['name']
        sha = row['head']
        url = f"https://api.github.com/repos/{repo_name}/commits/{sha}"
        
        try:
            resp = session.get(url, timeout=10)
            if resp.status_code == 200:
                files = resp.json().get('files', [])
                for f in files:
                    filename = f.get('filename', '').lower()
                    patch = f.get('patch', '')
                    if not patch: continue

                    # A. LEAK DETECTION (Shannon Entropy)
                    # We check the patch for high-entropy strings (secrets)
                    entropy = calculate_entropy(patch)
                    if entropy > 4.5:
                        # Tag as LEAK: (type, (data...))
                        yield ("LEAK", (repo_name, sha, float(entropy)))

                    # B. TECH TRENDS (Regex Parsing)
                    if 'requirements.txt' in filename:
                        libs = py_pattern.findall(patch)
                        for lib in libs:
                            if lib.lower() not in ['pip', 'setuptools', 'wheel']:
                                yield ("TREND", (lib.lower(), "Python", repo_name))
                    
                    elif 'package.json' in filename:
                        libs = js_pattern.findall(patch)
                        for lib in libs:
                            yield ("TREND", (lib.lower(), "JavaScript", repo_name))
        except:
            pass

# --- 3. MICRO-BATCH HANDLER (Driver) ---
def process_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    token = batch_df._sc.getConf().get("spark.app.github_token")
    
    # Execute the combined logic and cache results to avoid calling the API twice
    results_rdd = batch_df.rdd.mapPartitions(lambda it: analyze_partition(it, token)).cache()

    # --- SINK 1: TRENDS ---
    trends_rdd = results_rdd.filter(lambda x: x[0] == "TREND").map(lambda x: x[1])
    if not trends_rdd.isEmpty():
        trends_df = trends_rdd.toDF(["library_name", "language", "repo_name"])
        trends_df.withColumn("detected_at", current_timestamp()) \
            .write.format("bigquery") \
            .option("table", "githubleakmonitor-492112.watchtower_db.tech_trends") \
            .option("temporaryGcsBucket", "githubleakmonitor-492112-assets") \
            .mode("append").save()

    # --- SINK 2: LEAKS ---
    leaks_rdd = results_rdd.filter(lambda x: x[0] == "LEAK").map(lambda x: x[1])
    if not leaks_rdd.isEmpty():
        leaks_df = leaks_rdd.toDF(["repo_name", "commit_sha", "entropy_score"])
        leaks_df.withColumn("detected_at", current_timestamp()) \
            .write.format("bigquery") \
            .option("table", "githubleakmonitor-492112.watchtower_db.detected_leaks") \
            .option("temporaryGcsBucket", "githubleakmonitor-492112-assets") \
            .mode("append").save()
    
    results_rdd.unpersist()

# --- 4. SPARK SESSION & KAFKA SETUP ---
spark = SparkSession.builder.appName("Watchtower-Dual-Processor").getOrCreate()

kafka_url = spark.conf.get("spark.app.kafka_server")
kafka_key = spark.conf.get("spark.app.kafka_key")
kafka_secret = spark.conf.get("spark.app.kafka_secret")

schema = StructType([
    StructField("name", StringType()),
    StructField("head", StringType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_url) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .option("subscribe", "github-raw-events") \
    .load()

# --- 5. EXECUTION ---
clean_events = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

query = clean_events.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "gs://githubleakmonitor-492112-assets/checkpoints/dual_processor_v1") \
    .start()

query.awaitTermination()