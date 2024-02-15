import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    DB = os.environ.get('DB')
    DB_USER = os.environ.get('DB_USER')
    DB_PASS = os.environ.get('DB_PASS')
    DB_PORT = os.environ.get('DB_PORT')
    DB_HOST = os.environ.get('DB_HOST')
