#!/usr/bin/env python3
"""Set up test environment for rolling replacement script verification."""

import numpy as np
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusClient,
    connections,
    utility,
)
from pymilvus.client.types import ResourceGroupConfig

# Connect
connections.connect(alias="default", host="localhost", port="19530")
client = MilvusClient(uri="http://localhost:19530")

# Step 1: Create RGs
print("=== Creating Resource Groups ===")

# Update default RG to limits=0
utility.update_resource_groups(
    {
        "__default_resource_group": ResourceGroupConfig(
            requests={"node_num": 0},
            limits={"node_num": 0},
        ),
    }
)
print("Updated __default_resource_group: requests=0, limits=0")

# Create __recycle_resource_group with high limits to hold new QNs
rgs = utility.list_resource_groups()
recycle_config = ResourceGroupConfig(
    requests={"node_num": 0},
    limits={"node_num": 100000},
)
if "__recycle_resource_group" in rgs:
    utility.update_resource_groups({"__recycle_resource_group": recycle_config})
else:
    utility.create_resource_group("__recycle_resource_group", config=recycle_config)
print("Created/updated __recycle_resource_group: requests=0, limits=100000")

# Create replica RGs
for rg_name in ["rg_for_replica_1", "rg_for_replica_2"]:
    rgs = utility.list_resource_groups()
    config = ResourceGroupConfig(
        requests={"node_num": 2},
        limits={"node_num": 2},
    )
    if rg_name in rgs:
        utility.update_resource_groups({rg_name: config})
    else:
        utility.create_resource_group(rg_name, config=config)
    print(f"Created/updated {rg_name}: requests=2, limits=2")

# Wait for nodes to distribute
import time

time.sleep(5)

# Show RG status
print("\n=== Resource Group Status ===")
for rg in utility.list_resource_groups():
    info = utility.describe_resource_group(rg)
    nodes = info.nodes if hasattr(info, "nodes") else []
    print(f"  {rg}: available={info.num_available_node}, nodes={nodes}")

# Step 2: Create collection with data
print("\n=== Creating Collection ===")
collection_name = "test_rolling_replace"

# Drop if exists
if utility.has_collection(collection_name):
    utility.drop_collection(collection_name)

schema = CollectionSchema(
    fields=[
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=128),
    ]
)
collection = Collection(name=collection_name, schema=schema)
print(f"Created collection: {collection_name}")

# Insert 3000 rows
print("Inserting 3000 rows...")
ids = list(range(3000))
embeddings = np.random.random((3000, 128)).tolist()
collection.insert([ids, embeddings])
collection.flush()
print("Insert and flush complete.")

# Create index
print("Creating index...")
collection.create_index(
    field_name="embedding",
    index_params={"metric_type": "L2", "index_type": "FLAT"},
)
print("Index created.")

# Step 3: Load with 2 replicas using resource groups
print("\n=== Loading with 2 replicas ===")
collection.load(
    replica_number=2,
    _resource_groups=["rg_for_replica_1", "rg_for_replica_2"],
)
print("Load complete.")

# Wait for load
time.sleep(3)

# Show final state
print("\n=== Final State ===")
for rg in utility.list_resource_groups():
    info = utility.describe_resource_group(rg)
    nodes = info.nodes if hasattr(info, "nodes") else []
    print(f"  {rg}: available={info.num_available_node}, nodes={nodes}")

# Verify search works
print("\n=== Verifying Search ===")
results = collection.search(
    data=[embeddings[0]],
    anns_field="embedding",
    param={"metric_type": "L2"},
    limit=10,
)
print(f"Search returned {len(results[0])} results. Top hit id={results[0][0].id}")

print("\n=== Setup Complete ===")
