from db_utils import get_table_schema, save_snapshot
import argparse

def take_snapshot(table_name):
    schema = get_table_schema(table_name)
    save_snapshot(table_name, schema)
    print(f"Snapshot taken for table: {table_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Table name to snapshot")
    args = parser.parse_args()
    take_snapshot(args.table)