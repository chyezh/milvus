#!/usr/bin/env python3
"""
Test Upsert functionality in Milvus
This script tests the new Upsert message implementation
"""

import time
import numpy as np
from pymilvus import (
    connections,
    Collection,
    FieldSchema,
    CollectionSchema,
    DataType,
    utility,
)

# Configuration
COLLECTION_NAME = "upsert_test_collection"
DIM = 128
NUM_ENTITIES = 1000

def connect_to_milvus():
    """Connect to Milvus server"""
    print("Connecting to Milvus...")
    connections.connect(
        alias="default",
        host="localhost",
        port="19530"
    )
    print("Connected successfully!")

def create_collection():
    """Create a collection for testing"""
    print(f"\nCreating collection: {COLLECTION_NAME}")

    # Drop if exists
    if utility.has_collection(COLLECTION_NAME):
        print(f"Collection {COLLECTION_NAME} already exists, dropping it...")
        utility.drop_collection(COLLECTION_NAME)

    # Define schema
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=DIM),
        FieldSchema(name="value", dtype=DataType.INT64),
    ]
    schema = CollectionSchema(fields=fields, description="Upsert test collection")

    # Create collection
    collection = Collection(name=COLLECTION_NAME, schema=schema)
    print(f"Collection {COLLECTION_NAME} created successfully!")

    return collection

def test_basic_upsert(collection):
    """Test basic upsert functionality"""
    print("\n=== Test 1: Basic Upsert ===")

    # Insert initial data
    print(f"Inserting {NUM_ENTITIES} entities...")
    ids = list(range(NUM_ENTITIES))
    embeddings = np.random.random((NUM_ENTITIES, DIM)).tolist()
    values = [i * 10 for i in range(NUM_ENTITIES)]

    insert_data = [ids, embeddings, values]
    collection.insert(insert_data)
    collection.flush()

    print(f"Initial insert complete. Collection has {collection.num_entities} entities")

    # Upsert data (update existing + insert new)
    print("\nPerforming upsert operation...")
    upsert_ids = list(range(500, 1500))  # 500 existing + 500 new
    upsert_embeddings = np.random.random((1000, DIM)).tolist()
    upsert_values = [i * 100 for i in range(500, 1500)]  # Different values

    upsert_data = [upsert_ids, upsert_embeddings, upsert_values]
    collection.upsert(upsert_data)
    collection.flush()

    print(f"Upsert complete. Collection now has {collection.num_entities} entities")

    # Verify results
    assert collection.num_entities == 1500, f"Expected 1500 entities, got {collection.num_entities}"
    print("✓ Entity count is correct (1500)")

    # Create index and load collection for query
    print("\nCreating index and loading collection...")
    index_params = {
        "metric_type": "L2",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128}
    }
    collection.create_index(field_name="embedding", index_params=index_params)
    collection.load()

    # Query to verify updated values
    print("\nVerifying updated values...")
    results = collection.query(expr="id == 600", output_fields=["id", "value"])
    assert len(results) == 1, "Query should return 1 result"
    assert results[0]["value"] == 60000, f"Expected value 60000, got {results[0]['value']}"
    print(f"✓ Updated value verified: id=600, value={results[0]['value']}")

    # Query to verify new values
    results = collection.query(expr="id == 1200", output_fields=["id", "value"])
    assert len(results) == 1, "Query should return 1 result"
    assert results[0]["value"] == 120000, f"Expected value 120000, got {results[0]['value']}"
    print(f"✓ New value verified: id=1200, value={results[0]['value']}")

    collection.release()
    print("\n✓ Test 1 passed!")

def test_large_upsert(collection):
    """Test large upsert operation to verify message splitting"""
    print("\n=== Test 2: Large Upsert (Message Splitting) ===")

    # Clear collection
    utility.drop_collection(COLLECTION_NAME)
    collection = create_collection()

    # Insert initial data
    large_num = 10000
    print(f"Inserting {large_num} entities...")
    ids = list(range(large_num))
    embeddings = np.random.random((large_num, DIM)).tolist()
    values = [i for i in range(large_num)]

    insert_data = [ids, embeddings, values]
    collection.insert(insert_data)
    collection.flush()

    print(f"Initial insert complete. Collection has {collection.num_entities} entities")

    # Large upsert operation
    print("\nPerforming large upsert operation...")
    upsert_ids = list(range(5000, 15000))  # 5000 existing + 5000 new
    upsert_embeddings = np.random.random((10000, DIM)).tolist()
    upsert_values = [i * 100 for i in range(5000, 15000)]

    upsert_data = [upsert_ids, upsert_embeddings, upsert_values]
    collection.upsert(upsert_data)
    collection.flush()

    print(f"Upsert complete. Collection now has {collection.num_entities} entities")

    # Verify results
    assert collection.num_entities == 15000, f"Expected 15000 entities, got {collection.num_entities}"
    print("✓ Entity count is correct (15000)")

    print("\n✓ Test 2 passed!")

def test_concurrent_upsert(collection):
    """Test concurrent upsert operations"""
    print("\n=== Test 3: Concurrent Upsert ===")

    # Clear collection
    utility.drop_collection(COLLECTION_NAME)
    collection = create_collection()

    # Insert initial data
    print(f"Inserting {NUM_ENTITIES} entities...")
    ids = list(range(NUM_ENTITIES))
    embeddings = np.random.random((NUM_ENTITIES, DIM)).tolist()
    values = [i for i in range(NUM_ENTITIES)]

    insert_data = [ids, embeddings, values]
    collection.insert(insert_data)
    collection.flush()

    print(f"Initial insert complete. Collection has {collection.num_entities} entities")

    # Perform multiple upserts
    print("\nPerforming 5 consecutive upsert operations...")
    for i in range(5):
        upsert_ids = list(range(i * 100, (i + 1) * 100 + 500))
        upsert_embeddings = np.random.random((len(upsert_ids), DIM)).tolist()
        upsert_values = [id * (i + 1) for id in upsert_ids]

        upsert_data = [upsert_ids, upsert_embeddings, upsert_values]
        collection.upsert(upsert_data)
        print(f"  Upsert {i+1}/5 complete ({len(upsert_ids)} entities)")

    collection.flush()

    print(f"All upserts complete. Collection has {collection.num_entities} entities")

    # Verify the collection has expected number of entities
    expected_entities = 1000  # Maximum ID is around 1000
    assert collection.num_entities >= expected_entities, f"Expected at least {expected_entities} entities"
    print(f"✓ Entity count is valid ({collection.num_entities} entities)")

    print("\n✓ Test 3 passed!")

def test_upsert_with_deletion(collection):
    """Test upsert behavior with explicit deletions"""
    print("\n=== Test 4: Upsert with Deletion ===")

    # Clear collection
    utility.drop_collection(COLLECTION_NAME)
    collection = create_collection()

    # Insert initial data
    print(f"Inserting {NUM_ENTITIES} entities...")
    ids = list(range(NUM_ENTITIES))
    embeddings = np.random.random((NUM_ENTITIES, DIM)).tolist()
    values = [i for i in range(NUM_ENTITIES)]

    insert_data = [ids, embeddings, values]
    collection.insert(insert_data)
    collection.flush()

    print(f"Initial insert complete. Collection has {collection.num_entities} entities")

    # Delete some entities
    print("\nDeleting entities 100-199...")
    expr = "id >= 100 && id < 200"
    collection.delete(expr)
    collection.flush()

    print(f"After deletion. Collection has {collection.num_entities} entities")

    # Upsert to bring back some deleted entities
    print("\nUpserting entities 150-250 (50 deleted + 50 existing + 50 new)...")
    upsert_ids = list(range(150, 250))
    upsert_embeddings = np.random.random((100, DIM)).tolist()
    upsert_values = [i * 1000 for i in range(150, 250)]

    upsert_data = [upsert_ids, upsert_embeddings, upsert_values]
    collection.upsert(upsert_data)
    collection.flush()

    print(f"After upsert. Collection has {collection.num_entities} entities")

    # Verify count: initial 1000 - 100 deleted + 50 new = 950
    expected = 950
    assert collection.num_entities == expected, f"Expected {expected} entities, got {collection.num_entities}"
    print(f"✓ Entity count is correct ({expected})")

    print("\n✓ Test 4 passed!")

def main():
    """Main test execution"""
    print("=" * 60)
    print("Milvus Upsert Functionality Test")
    print("=" * 60)

    try:
        # Connect to Milvus
        connect_to_milvus()

        # Create collection
        collection = create_collection()

        # Run tests
        test_basic_upsert(collection)
        test_large_upsert(collection)
        test_concurrent_upsert(collection)
        test_upsert_with_deletion(collection)

        # Cleanup
        print("\n" + "=" * 60)
        print("Cleaning up...")
        utility.drop_collection(COLLECTION_NAME)
        print(f"Collection {COLLECTION_NAME} dropped")

        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        connections.disconnect("default")

    return 0

if __name__ == "__main__":
    exit(main())
