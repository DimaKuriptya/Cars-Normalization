import pathlib
import configparser
import sqlalchemy


parser = configparser.ConfigParser()
parser.read(pathlib.Path(__file__).parent / 'config.conf')

SERVER = parser.get('db_connection', 'server')
DATABASE = parser.get('db_connection', 'database')
DRIVER = parser.get('db_connection', 'driver')
USERNAME = parser.get('db_connection', 'username')
PASSWORD = parser.get('db_connection', 'password')

DATABASE_CONNECTION = f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}&TrustServerCertificate=yes'
engine = sqlalchemy.create_engine(DATABASE_CONNECTION, future=True)
