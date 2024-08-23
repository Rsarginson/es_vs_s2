from elasticsearch import Elasticsearch
import json
import random
import time
from statistics import mean, median

# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200'])  # Update with your Elasticsearch host if different

# Index name
index_name = 'video_game_embeddings'

def knn_search(vector, k=5):
    query = {
        "knn": {
            "field": "vector",
            "query_vector": vector,
            "k": k,
            "num_candidates": 100
        },
        "_source": ["id", "url", "paragraph"]
    }
    
    start_time = time.time()
    results = es.search(index=index_name, body=query)
    end_time = time.time()
    
    latency = end_time - start_time
    
    return results, latency

def run_knn_searches(num_searches=100, k=5):
    latencies = []
    start_time = time.time()
    
    for i in range(num_searches):
        # Generate a sample vector (replace this with actual vectors for meaningful results)
        sample_vector = [random.uniform(-1, 1) for i in range(1536)]
        
        # Perform kNN search
        i, latency = knn_search(sample_vector, k)
        latencies.append(latency)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Calculate metrics
    qps = num_searches / total_time
    avg_latency = mean(latencies)
    median_latency = median(latencies)
    p95_latency = sorted(latencies)[int(0.95 * len(latencies))]
    p99_latency = sorted(latencies)[int(0.99 * len(latencies))]
    
    print(f"Total searches: {num_searches}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Queries Per Second (QPS): {qps:.2f}")
    print(f"Average latency: {avg_latency:.4f} seconds")
    print(f"Median latency: {median_latency:.4f} seconds")
    print(f"95th percentile latency: {p95_latency:.4f} seconds")
    print(f"99th percentile latency: {p99_latency:.4f} seconds")

# Run the kNN searches and print metrics
index_stats = es.indices.stats(index=index_name)
index_size_bytes = index_stats['indices'][index_name]['total']['store']['size_in_bytes']
index_size_mb = index_size_bytes / (1024 * 1024)

print(f"Index size: {index_size_mb:.2f} MB")

run_knn_searches(num_searches=100, k=5)