#!/usr/bin/env python3
"""Locate a dataset URN by name and platform using the DataHub GraphQL API."""
import json
import os
import sys
import urllib.request

GRAPHQL_ENDPOINT = os.environ.get("DATAHUB_GMS", "http://localhost:8080").rstrip("/") + "/api/graphql"
TOKEN = os.environ.get("DATAHUB_TOKEN")

if len(sys.argv) < 2:
    print("Usage: scripts/find_dataset_urn.py <name> [platform]", file=sys.stderr)
    sys.exit(1)

name = sys.argv[1]
platform = sys.argv[2] if len(sys.argv) > 2 else "postgres"
platform_urn = f"urn:li:dataPlatform:{platform}"

query = """
query search($input: SearchInput!) {
  search(input: $input) {
    entities {
      entity {
        urn
        ... on Dataset {
          name
          platform {
            urn
          }
        }
      }
    }
  }
}
"""

variables = {
    "input": {
        "type": "DATASET",
        "query": name,
        "start": 0,
        "count": 20,
        "filters": [
            {"field": "platform", "value": platform_urn},
        ],
    }
}

payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
request = urllib.request.Request(GRAPHQL_ENDPOINT, data=payload, method="POST")
request.add_header("Content-Type", "application/json")
if TOKEN:
    request.add_header("Authorization", f"Bearer {TOKEN}")

with urllib.request.urlopen(request) as response:
    data = json.loads(response.read().decode("utf-8"))

if "errors" in data:
    print(json.dumps(data["errors"], indent=2), file=sys.stderr)
    sys.exit(1)

entities = data.get("data", {}).get("search", {}).get("entities", [])
if not entities:
    print("No matching datasets found", file=sys.stderr)
    sys.exit(1)

for entity in entities:
    urn = entity.get("entity", {}).get("urn")
    dataset = entity.get("entity", {}).get("name")
    print(f"{urn}\t{dataset}")
