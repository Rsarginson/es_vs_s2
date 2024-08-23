import csv
from elasticsearch import Elasticsearch, helpers
import json

# Elasticsearch connection details
es = Elasticsearch(['http://localhost:9200'])  # Update with your Elasticsearch host if different

# Index name
index_name = 'video_game_embeddings'

# Define the mapping for the index
mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "long"},  # Changed from "keyword" to "long"
            "url": {"type": "keyword"},
            "paragraph": {"type": "text"},
            "vector": {
                "type": "dense_vector",
                "dims": 1536
            }
        }
    }
}

# Delete the existing index if it exists
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Deleted existing index: {index_name}")

# Create the index with the new mapping
es.indices.create(index=index_name, body=mapping)
print(f"Created index: {index_name}")

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
                    print(f"Row data: {full_text[:200]}...")
                    continue

                # Extract other fields
                text_part = full_text[:full_text.find('[')]
                parts = text_part.split(',')
                if len(parts) < 3:
                    print(f"Row {i} has insufficient data")
                    print(f"Row data: {full_text[:200]}...")
                    continue

                doc = {
                    '_index': index_name,
                    '_source': {
                        'id': int(parts[0].strip()),  # Convert to integer
                        'url': parts[1].strip(),
                        'paragraph': ','.join(parts[2:]).strip(),
                        'vector': vector
                    }
                }
                print(f"Processed row {i}")
                yield doc
            except Exception as e:
                print(f"Error parsing row {i}: {e}")
                print(f"Row data: {full_text[:200]}...")  # Print first 200 chars

# Bulk insert data
try:
    for success, info in helpers.parallel_bulk(es, read_csv(), chunk_size=500, raise_on_error=False):
        if not success:
            print(f'A document failed: {info}')
except Exception as e:
    print(f"Error during bulk insert: {str(e)}")

# Refresh the index to make the documents searchable immediately
es.indices.refresh(index=index_name)

print("Data ingestion completed")