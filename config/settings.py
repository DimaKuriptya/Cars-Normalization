import pathlib
import configparser
import sqlalchemy


parser = configparser.ConfigParser()
parser.read(pathlib.Path(__file__).parent / 'config.conf')

SERVER = parser.get('db_connection', 'server')
DATABASE = parser.get('db_connection', 'database')
DRIVER = parser.get('db_connection', 'driver')

engine = sqlalchemy.create_engine(rf'mssql+pyodbc://{SERVER}/{DATABASE}?trusted_connection=yes&driver={DRIVER}')
