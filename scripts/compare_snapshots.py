
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from scripts.db_utils import get_table_schema, get_latest_snapshot, log_schema_change
import json
from datetime import datetime


###########################################################################
def detect_changes(table_name):
    current_schema = get_table_schema(table_name)
    last_snapshot = get_latest_snapshot(table_name)
    
    if not last_snapshot:
        print(f"No previous snapshot found for table {table_name}")
        return []
    
    changes = compare_schemas(last_snapshot, current_schema)
    
    if changes:
        print(f"Detected {len(changes)} changes in {table_name}:")
        for change in changes:
            print(json.dumps(change, indent=2))
            log_schema_change(table_name, change['change_type'], change)
    else:
        print(f"No changes detected in {table_name}")
    
    return changes
###########################################################################
def compare_schemas(old_schema, new_schema):
    changes = []
    old_cols = {col['column_name']: col for col in old_schema['columns']}
    new_cols = {col['column_name']: col for col in new_schema['columns']}

    # Detect added columns
    for col_name in set(new_cols) - set(old_cols):
        changes.append({
            'change_type': 'column_added',
            'column_name': col_name,
            'data_type': new_cols[col_name]['data_type'],
            'is_nullable': new_cols[col_name]['is_nullable'],
            'column_default': new_cols[col_name]['column_default']
        })

    # Detect removed columns
    for col_name in set(old_cols) - set(new_cols):
        changes.append({
            'change_type': 'column_removed',
            'column_name': col_name
        })

    # Detect modified columns
    for col_name in set(old_cols) & set(new_cols):
        if old_cols[col_name] != new_cols[col_name]:
            changes.append({
                'change_type': 'column_modified',
                'column_name': col_name,
                'old_type': old_cols[col_name]['data_type'],
                'new_type': new_cols[col_name]['data_type']
            })
    
    return changes

########################################################################################
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Table name to check for changes")
    args = parser.parse_args()
    
    changes = detect_changes(args.table)
    if changes:
        print(f"Detected {len(changes)} changes in table {args.table}:")
        for change in changes:
            print(json.dumps(change, indent=2))
    else:
        print(f"No changes detected in table {args.table}")