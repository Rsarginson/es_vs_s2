# Elasticsearch vs SingleStore Vector Search Benchmark
This repository contains a benchmarking demo comparing the performance of Elasticsearch and SingleStore for vector and hybrid searches. The benchmark focuses on high-dimensional vectors and large datasets to simulate real-world scenarios.
# Overview
This benchmark compares the performance of Elasticsearch and SingleStore in the following areas:
Vector search (cosine similarity)
Hybrid search (combination of vector similarity and keyword matching)
Index creation and data ingestion
Query performance and latency
Dataset
wikipedia video game articles found via web-scrape. 
Records contained in a CSV file of approximately 800 MB, found in an open s3 bucket here - s3://wikipedia-video-game-data/video-game-embeddings(1).csv
Each record contains:
Text fields (paragraph, URL)
Numeric fields (id)
Vector field (1536 dimensions)

Mock vectors created within generate_vectors.py
Indexing into elasticsearch done in elastic_index.py
KNN search done in elastic_knn.py
ANN search done in elastic_ann.py

# Prerequisites
Docker
Python 3.8+
Elasticsearch 8.x
SingleStore 8.x
