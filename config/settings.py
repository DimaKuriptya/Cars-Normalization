import sqlalchemy


SERVER = r'Dima\ADMIN'
DATABASE = 'normalization'
DRIVER = 'ODBC Driver 17 for SQL Server'

engine = sqlalchemy.create_engine(rf'mssql+pyodbc://{SERVER}/{DATABASE}?trusted_connection=yes&driver={DRIVER}')
