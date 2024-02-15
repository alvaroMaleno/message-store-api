from psycopg2.extras import RealDictCursor
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

pg_connection_dict = {
    'dbname': os.environ.get("DB"),
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASS"),
    'port': os.environ.get("DB_PORT"),
    'host': os.environ.get("DB_HOST")
}


def query_to_json(query):
    connection = psycopg2.connect(**pg_connection_dict)
    with connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            messages = cursor.fetchall()
    return json.dumps(messages, default=str)


def insert(query):
    conn = None
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    if conn is not None:
        conn.close()

    return 'Created'
