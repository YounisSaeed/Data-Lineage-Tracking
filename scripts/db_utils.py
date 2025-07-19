import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime
import os
from dotenv import load_dotenv
import logging
import time

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#######################################################################
def get_connection():
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host="postgres",
                database=os.getenv("POSTGRES_DB", "data_catalog"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
                connect_timeout=10
            )
            conn.autocommit = True
            logger.info("Database connection established")
            return conn
        except psycopg2.OperationalError as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to connect after {max_retries} attempts")
                raise
            logger.warning(f"Connection failed (attempt {attempt + 1}): {str(e)}")
            time.sleep(retry_delay)

#######################################################################
def table_exists(table_name):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    );
                """, (table_name,))
                return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error checking table existence: {str(e)}")
        return False

#######################################################################
def get_table_schema(table_name):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable, 
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """
                cursor.execute(query, (table_name,))
                columns = cursor.fetchall()
                
                if not columns:
                    raise ValueError(f"Table {table_name} not found or has no columns")
                
                pk_query = """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_name = %s
                      AND tc.constraint_type = 'PRIMARY KEY';
                """
                cursor.execute(pk_query, (table_name,))
                primary_keys = [row['column_name'] for row in cursor.fetchall()]
                
                fk_query = """
                    SELECT
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.table_name = %s
                      AND tc.constraint_type = 'FOREIGN KEY';
                """
                cursor.execute(fk_query, (table_name,))
                foreign_keys = cursor.fetchall()
                
                return {
                    'table_name': table_name,
                    'columns': columns,
                    'primary_keys': primary_keys,
                    'foreign_keys': foreign_keys,
                    'timestamp': datetime.now().isoformat()
                }
    except Exception as e:
        logger.error(f"Error getting schema for table {table_name}: {str(e)}")
        raise

#######################################################################
def log_schema_change(table_name, change_type, change_details):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO schema_change_log 
                    (table_name, change_type, change_details)
                    VALUES (%s, %s, %s)
                    RETURNING log_id;
                """
                cursor.execute(query, (table_name, change_type, json.dumps(change_details)))
                log_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Logged change {log_id} for table {table_name}")
                return log_id
    except Exception as e:
        logger.error(f"Error logging change for {table_name}: {str(e)}")
        raise

#######################################################################
def save_snapshot(table_name, snapshot_data):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO table_snapshots 
                    (table_name, snapshot_data)
                    VALUES (%s, %s)
                """
                cursor.execute(query, (
                    table_name,
                    json.dumps(snapshot_data) if not isinstance(snapshot_data, str) else snapshot_data
                ))
                conn.commit()
                logger.info(f"Saved snapshot for table {table_name}")
    except Exception as e:
        logger.error(f"Error saving snapshot for {table_name}: {str(e)}")
        raise

#######################################################################
def apply_schema_changes(table_name, changes):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for change in changes:
                    if is_change_already_applied(cursor, table_name, change):
                        logger.info(f"Change already applied, skipping: {change}")
                        continue
                    
                    log_id = log_schema_change(
                        table_name, 
                        change['change_type'], 
                        change
                    )
                    
                    try:
                        query = build_alter_statement(table_name, change)
                        cursor.execute(query)
                        logger.info(f"Successfully applied {change['change_type']} to {table_name}")
                        
                    except Exception as e:
                        logger.error(f"Failed to apply {change['change_type']} to {table_name}: {str(e)}")
                        raise
                
                conn.commit()
    except Exception as e:
        logger.error(f"Error applying changes to {table_name}: {str(e)}")
        raise

#######################################################################
def is_change_already_applied(cursor, table_name, change):
    try:
        if change['change_type'] == 'column_added':
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (table_name, change['column_name']))
            return cursor.fetchone() is not None
            
        elif change['change_type'] == 'column_removed':
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (table_name, change['column_name']))
            return cursor.fetchone() is None
            
        elif change['change_type'] == 'column_modified':
            cursor.execute("""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (table_name, change['column_name']))
            result = cursor.fetchone()
            return result and result[0] == change['new_type']
            
    except Exception as e:
        logger.warning(f"Error checking if change is applied: {str(e)}")
        return False

#######################################################################
def build_alter_statement(table_name, change):
    if change['change_type'] == 'column_added':
        query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
            sql.Identifier(table_name),
            sql.Identifier(change['column_name']),
            sql.SQL(change['data_type'])
        )
        if change.get('is_nullable', 'YES') == 'NO':
            query = sql.SQL("{} NOT NULL").format(query)
        if change.get('column_default'):
            query = sql.SQL("{} DEFAULT {}").format(
                query,
                sql.SQL(change['column_default'])
            )
        return query
        
    elif change['change_type'] == 'column_removed':
        return sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
            sql.Identifier(table_name),
            sql.Identifier(change['column_name'])
        )
        
    elif change['change_type'] == 'column_modified':
        return sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE {}").format(
            sql.Identifier(table_name),
            sql.Identifier(change['column_name']),
            sql.SQL(change['new_type'])
        )

#######################################################################
def get_latest_snapshot(table_name):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT snapshot_data::text as snapshot_data 
                    FROM table_snapshots 
                    WHERE table_name = %s 
                    ORDER BY created_at DESC 
                    LIMIT 1
                """
                cursor.execute(query, (table_name,))
                result = cursor.fetchone()
                if result and result['snapshot_data']:
                    return json.loads(result['snapshot_data'])
                return None
    except Exception as e:
        logger.error(f"Error getting snapshot for {table_name}: {str(e)}")
        raise