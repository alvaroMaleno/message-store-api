import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from flask import Flask

load_dotenv()  # loads variables from .env file into environment

app = Flask(__name__)
pg_connection_dict = {
    'dbname': os.environ.get("DB"),
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASS"),
    'port': os.environ.get("DB_PORT"),
    'host': os.environ.get("DB_HOST")
}
print(pg_connection_dict)
connection = psycopg2.connect(**pg_connection_dict)

ALL_MESSAGES = """SELECT * FROM message_store.message_store.messages;"""


@app.get("/api/messages")
def get_all_messages():
    with connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(ALL_MESSAGES)
            messages = cursor.fetchall()
    return json.dumps(messages, default=str)
