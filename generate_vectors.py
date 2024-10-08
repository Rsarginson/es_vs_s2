import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch(['http://localhost:9200'])

## Functions to create a normalized, random vector of dimensionality 1536

def randbetween(a, b):
    return np.random.uniform(a, b)

def gen_vector(length):
    if length < 2:
        raise ValueError("length too short: {}".format(length))
    return [randbetween(-1, 1) for _ in range(length)]

def normalize(v):
    length = np.linalg.norm(v)
    return [x / length for x in v]

def nrandv1536():
    vector = gen_vector(1536)
    return normalize(vector)

# Script to populate an elasticsearch index with vectors

index_name = 'video_game_embeddings'
response = es.count(index=index_name)
print("Number of documents:", response['count'])
starting_point = response['count']


def generate_documents(num_docs):
    for i in range(starting_point+1, starting_point + num_docs):
        yield {
            "_index": index_name,
            "_id": i,
            "_source": {
                "id": i,
                "vector": nrandv1536()
            }
        }

# Insert vectors into Elasticsearch
num_docs = 60975  # Adjust the number of documents as needed
bulk(es, generate_documents(num_docs))

print("done")   
