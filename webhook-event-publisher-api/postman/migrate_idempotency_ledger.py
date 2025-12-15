#!/usr/bin/env python3
"""
Migration script to rename EVENT_ID to eventID and EVENT_STATUS to eventStatus
in the EVENT_IDEMPOTENCY_LEDGER DynamoDB table.
"""

import json
import subprocess
import sys
from typing import Dict, Any

TABLE_NAME = "EVENT_IDEMPOTENCY_LEDGER"
ENDPOINT_URL = "http://localhost:8000"


def run_aws_cli(command: list) -> Dict[Any, Any]:
    """Run AWS CLI command and return JSON output."""
    cmd = ["aws", "dynamodb"] + command + ["--endpoint-url", ENDPOINT_URL, "--output", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(cmd)}")
        print(f"Error: {e.stderr}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        sys.exit(1)


def get_table_key_schema():
    """Get the table's key schema."""
    result = run_aws_cli(["describe-table", "--table-name", TABLE_NAME])
    return result["Table"]["KeySchema"]


def get_partition_key():
    """Get the partition key attribute name."""
    key_schema = get_table_key_schema()
    for key in key_schema:
        if key["KeyType"] == "HASH":
            return key["AttributeName"]
    return None


def scan_all_items():
    """Scan all items from the table."""
    items = []
    last_evaluated_key = None
    
    while True:
        cmd = ["scan", "--table-name", TABLE_NAME]
        if last_evaluated_key:
            cmd.extend(["--exclusive-start-key", json.dumps(last_evaluated_key)])
        
        result = run_aws_cli(cmd)
        items.extend(result.get("Items", []))
        
        last_evaluated_key = result.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    
    return items


def migrate_item(item: Dict, partition_key: str):
    """Migrate a single item by creating new item with renamed attributes."""
    # Extract values
    event_id = item.get("EVENT_ID", {}).get("S", "")
    event_status = item.get("EVENT_STATUS", {}).get("S", "")
    event_schemaid = item.get("EVENT_SCHEMAID", {}).get("S", "")
    creation_ts = item.get("CREATION_TIMESTAMP", {}).get("S", "")
    updated_ts = item.get("UPDATED_TIMESTAMP", {}).get("S", "")
    
    if not event_id:
        print(f"Skipping item without EVENT_ID")
        return
    
    # Get partition key value
    pk_value = item.get(partition_key, {}).get("S", "")
    if not pk_value:
        print(f"Skipping item without partition key: {partition_key}")
        return
    
    # Build new item
    new_item = {
        partition_key: item[partition_key],  # Keep the partition key as is
        "eventID": {"S": event_id},
        "eventStatus": {"S": event_status}
    }
    
    if creation_ts:
        new_item["CREATION_TIMESTAMP"] = {"S": creation_ts}
    
    if updated_ts:
        new_item["UPDATED_TIMESTAMP"] = {"S": updated_ts}
    
    if event_schemaid:
        new_item["EVENT_SCHEMAID"] = {"S": event_schemaid}
    
    # Put new item
    put_cmd = [
        "put-item",
        "--table-name", TABLE_NAME,
        "--item", json.dumps(new_item)
    ]
    run_aws_cli(put_cmd)
    
    # Delete old attributes (if EVENT_ID is not the partition key)
    if partition_key != "EVENT_ID":
        key_item = {partition_key: item[partition_key]}
        update_cmd = [
            "update-item",
            "--table-name", TABLE_NAME,
            "--key", json.dumps(key_item),
            "--update-expression", "REMOVE EVENT_ID, EVENT_STATUS"
        ]
        run_aws_cli(update_cmd)
    
    print(f"Migrated: eventID={event_id}, eventStatus={event_status}")


def main():
    print(f"Starting migration of {TABLE_NAME} table...")
    print(f"Renaming: EVENT_ID -> eventID, EVENT_STATUS -> eventStatus")
    print()
    
    # Check if table exists
    try:
        key_schema = get_table_key_schema()
        partition_key = get_partition_key()
        print(f"Table found. Partition key: {partition_key}")
        print()
    except Exception as e:
        print(f"Error: Table {TABLE_NAME} does not exist or cannot be accessed!")
        print(f"Error: {e}")
        sys.exit(1)
    
    # Check if EVENT_ID is the partition key
    if partition_key == "EVENT_ID":
        print("WARNING: EVENT_ID is the partition key!")
        print("To rename the partition key, you need to recreate the table.")
        print()
        response = input("Do you want to proceed with recreating the table? (yes/no): ")
        if response.lower() != "yes":
            print("Migration cancelled.")
            sys.exit(0)
        
        # This is a complex operation - would need to create new table
        print("This requires manual steps:")
        print("1. Create new table with 'eventID' as partition key")
        print("2. Migrate all data")
        print("3. Delete old table")
        print()
        print("Use the shell script (migrate_idempotency_ledger.sh) for this scenario.")
        sys.exit(0)
    
    # Scan and migrate items
    print("Scanning items...")
    items = scan_all_items()
    print(f"Found {len(items)} items to migrate")
    print()
    
    migrated_count = 0
    for item in items:
        try:
            migrate_item(item, partition_key)
            migrated_count += 1
        except Exception as e:
            print(f"Error migrating item: {e}")
            continue
    
    print()
    print(f"Migration complete! Migrated {migrated_count} items.")
    print()
    
    # Verify
    print("Verifying final state (showing first item)...")
    result = run_aws_cli(["scan", "--table-name", TABLE_NAME, "--limit", "1"])
    if result.get("Items"):
        print(json.dumps(result["Items"][0], indent=2))
    else:
        print("No items found")


if __name__ == "__main__":
    main()

