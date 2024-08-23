import csv
from elasticsearch import Elasticsearch, helpers
import json
import time

# Elasticsearch connection details
es = Elasticsearch(['http://localhost:9200'])  # Update with your Elasticsearch host if different

# Index name
index_name = 'video_game_embeddings'

# Define the mapping for the index
mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "long"},
            "url": {"type": "keyword"},
            "paragraph": {"type": "text"},
            "vector": {
                "type": "dense_vector",
                "dims": 1536
            }
        }
    }
}

def extract_vector(text):
    start = text.find('[')
    end = text.rfind(']')
    if start != -1 and end != -1:
        vector_str = text[start:end+1]
        try:
            vector = json.loads(vector_str)
            if isinstance(vector, list) and len(vector) == 1536 and all(isinstance(x, (int, float)) for x in vector):
                return vector
        except json.JSONDecodeError:
            pass
    return None

# Function to read the CSV and yield documents
def read_csv():
    with open('video-game-embeddings.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        for i, row in enumerate(reader):
            try:
                full_text = ','.join(row)
                vector = extract_vector(full_text)
                
                if vector is None:
                    print(f"Row {i} has invalid or missing vector data")
                    continue

                # Extract other fields
                text_part = full_text[:full_text.find('[')]
                parts = text_part.split(',')
                if len(parts) < 3:
                    print(f"Row {i} has insufficient data")
                    continue

                doc = {
                    '_index': index_name,
                    '_source': {
                        'id': int(parts[0].strip()),
                        'url': parts[1].strip(),
                        'paragraph': ','.join(parts[2:]).strip(),
                        'vector': vector
                    }
                }
                yield doc
            except Exception as e:
                print(f"Error parsing row {i}: {e}")

def index_documents():
    start_time = time.time()

    # Delete the existing index if it exists
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")

    # Create the index with the new mapping
    es.indices.create(index=index_name, body=mapping)
    print(f"Created index: {index_name}")

    # Bulk insert data
    try:
        success, failed = helpers.bulk(es, read_csv(), chunk_size=500)
        print(f"Successfully indexed {success} documents")
        if failed:
            print(f"Failed to index {len(failed)} documents")
    except Exception as e:
        print(f"Error during bulk insert: {str(e)}")

    # Refresh the index to make the documents searchable immediately
    es.indices.refresh(index=index_name)

    end_time = time.time()
    total_time = end_time - start_time

    print(f"Total index build time: {total_time:.2f} seconds")

    # Get index size
    index_stats = es.indices.stats(index=index_name)
    index_size_bytes = index_stats['indices'][index_name]['total']['store']['size_in_bytes']
    index_size_mb = index_size_bytes / (1024 * 1024)
    print(f"Index size: {index_size_mb:.2f} MB")

# Run the indexing process
index_documents()