import openpyxl
import pathlib
import pandas as pd
import pyodbc
from datetime import datetime


# # Connection details
# server = 'Dima\ADMIN'
# database = 'normalization'
# username = 'dima'
# password = ''

# # Connection String
# conn_str = (
#     'DRIVER={ODBC Driver 5.1.1 for SQL Server};'  # Specify correct ODBC driver version
#     f'SERVER={server};'
#     f'DATABASE={database};'
#     f'UID={username};'
#     f'PWD={password}'
# )

# # Connect to the database
# try:
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()

#     # Example SQL request
#     sql_query = "SELECT * FROM [normalized].[Country]"  # Replace with your desired query
#     cursor.execute(sql_query)

#     # Process results
#     rows = cursor.fetchall()
#     for row in rows:
#         print(row)


# except pyodbc.Error as ex:
#     print("Connection error:", ex)
# finally:
#     if conn:
#         conn.close()


def validate_date(raw_date):
    try:
        if isinstance(raw_date, str):
            date_obj = datetime.strptime(raw_date, "%d/%m/%Y")
        else:
            date_obj = raw_date

        ready_date = date_obj.strftime("%d-%m-%Y")

    except ValueError:
        print("Invalid date format")

    return ready_date


def build_df(row, id):
    data = {}
    data['id'] = id
    data['brand'] = row[0].value
    # data['models'] = row[1].value.split(', ')
    data['ceo'] = row[2].value.strip()
    data['country'] = row[3].value
    # data['headquarters'] = row[4].value.split(', ')
    # data['engine_types'] = row[5].value.split(', ')
    # data['founders'] = list(map(lambda x: x.strip(), row[6].value.replace('\n', '').replace('\t', '').split(',')))
    data['foundation'] = validate_date(row[7].value)
    data['ev'] = True if row[8].value == 'Y' else False
    # data['operating_income'] = row[9].value.split('\n')

    return data


url = pathlib.Path(__file__).parent / 'Table for Normalization.xlsx'
workbook = openpyxl.load_workbook(url)
worksheet = workbook.active

START_ROW = 5
rows = worksheet.iter_rows(min_row=START_ROW)


total_data = []
for id, row in enumerate(rows):
    row_df = build_df(row, id)
    total_data.append(row_df)

df = pd.DataFrame(total_data)
print(df)
