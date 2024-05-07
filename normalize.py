from datetime import datetime
import openpyxl
import pathlib
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from settings import engine


def prepare_database():
    normalize_url = pathlib.Path(__file__).parent / 'sql_scripts/normalize.sql'
    countries_url = pathlib.Path(__file__).parent / 'sql_scripts/countries.sql'
    engines_url = pathlib.Path(__file__).parent / 'sql_scripts/engines.sql'

    with engine.connect() as con:
        for path in [normalize_url, countries_url, engines_url]:
            with open(path, encoding='utf-8') as file:
                query = text(file.read())
                con.execute(query)
                con.commit()


def load_to_database(df: pd.DataFrame, table_name: str, connection: sqlalchemy.Engine = engine) -> None:
    try:
        df.to_sql(table_name, schema='normalized', con=connection, if_exists='append', index=False)
    except Exception as e:
        print(f'Unable to load the dataframe into {table_name} table: {e}')


def validate_date(raw_date: str | datetime):
    try:
        if isinstance(raw_date, str):
            date_obj = datetime.strptime(raw_date, "%d/%m/%Y")
        else:
            date_obj = raw_date

        ready_date = date_obj.strftime("%Y-%m-%d")

    except ValueError:
        print("Invalid date format")

    return ready_date


def build_df(row, id: int):
    data = {}
    data['id'] = id
    data['brand'] = row[0].value
    data['ceo'] = row[2].value.strip()
    data['country_id'] = row[3].value
    data['foundation'] = validate_date(row[7].value)
    data['ev'] = True if row[8].value == 'Y' else False

    data['models'] = row[1].value.split(', ')
    data['headquarters'] = row[4].value.rsplit(', ', 1)
    data['engine_types'] = row[5].value.split(', ')
    data['founders'] = list(map(lambda x: x.strip(), row[6].value.replace('\n', '').replace('\t', '').split(',')))
    data['operating_income'] = row[9].value.split('\n')

    return data


def load_company_table(df: pd.DataFrame):
    table_name = 'Company'

    try:
        country_df = pd.read_sql(sql="SELECT id, nicename FROM normalized.Country", con=engine)
    except Exception as e:
        print('Unable to get countries: ' + e)

    for _, row in df.iterrows():
        df['country_id'] = country_df.loc[country_df['nicename'] == row['country_id'], 'id'].values[0]

    with engine.begin() as conn:
        conn.exec_driver_sql(f"SET IDENTITY_INSERT [normalized].[{table_name}] ON")

        try:
            load_to_database(df, table_name, connection=conn)
        finally:
            conn.exec_driver_sql(f"SET IDENTITY_INSERT [normalized].[{table_name}] OFF")

def load_engine_type_table(df: pd.DataFrame):
    try:
        engine_df = pd.read_sql(sql="SELECT id, type FROM normalized.Engine", con=engine)
    except Exception as e:
        print('Unable to get engine types: ' + e)

    engine_type_data = []

    for _, row in df.iterrows():
        for engine_type in row['engine_types']:
            engine_id = engine_df.loc[engine_df['type'] == engine_type, 'id'].values[0]
            engine_type_data.append({'company_id': row['id'], 'engine_id': engine_id})

    engine_type_df = pd.DataFrame(engine_type_data)
    load_to_database(engine_type_df, 'EngineType')


def load_model_table(df: pd.DataFrame):
    models_data = []

    for _, row in df.iterrows():
        for model in row['models']:
            models_data.append({'company_id': row['id'], 'name': model})

    model_df = pd.DataFrame(models_data)
    load_to_database(model_df, 'Model')


def load_founder_table(df: pd.DataFrame):
    founders_data = []

    for _, row in df.iterrows():
        for full_name in row['founders']:
            founders_data.append({'company_id': row['id'], 'full_name': full_name})

    founder_df = pd.DataFrame(founders_data)
    load_to_database(founder_df, 'Founder')


def load_operating_income_table(df: pd.DataFrame):
    income_data = []

    for _, row in df.iterrows():
        for yearly_info in row['operating_income']:
            yearly_info = yearly_info.split(': ')
            income_data.append({'company_id': row['id'], 'year': int(yearly_info[0]), 'income': int(yearly_info[1])})

    operating_income_df = pd.DataFrame(income_data)
    load_to_database(operating_income_df, 'OperatingIncome')


def load_headquarter_table(df: pd.DataFrame):
    headquarters_data = []

    try:
        country_df = pd.read_sql(sql="SELECT id, nicename FROM normalized.Country", con=engine)
    except Exception as e:
        print('Unable to get countries: ' + e)

    for _, row in df.iterrows():
        row['headquarters'][-1] = country_df.loc[country_df['nicename'] == row['headquarters'][-1], 'id'].values[0] # Transforming str country to foreign key
        headquarters_data.append({'company_id': row['id'], 'country_id': row['headquarters'][-1], 'city': row['headquarters'][0]})

    headquarters_df = pd.DataFrame(headquarters_data)
    load_to_database(headquarters_df, 'Headquarter')


def get_df_from_excel(url):
    workbook = openpyxl.load_workbook(url)
    worksheet = workbook.active

    start_row = 5
    rows = worksheet.iter_rows(min_row=start_row)
    total_data = []

    for id, row in enumerate(rows):
        row_df = build_df(row, id)
        total_data.append(row_df)

    df = pd.DataFrame(total_data)
    return df


def main():
    url = pathlib.Path(__file__).parent / 'xlsx_files/Table for Normalization.xlsx'
    df = get_df_from_excel(url)

    prepare_database()

    load_company_table(df.loc[:, ['id', 'brand', 'ceo', 'country_id', 'foundation', 'ev']])
    load_operating_income_table(df.loc[:, ['id', 'operating_income']])
    load_model_table(df.loc[:, ['id', 'models']])
    load_founder_table(df.loc[:, ['id', 'founders']])
    load_headquarter_table(df.loc[:, ['id', 'headquarters']])
    load_engine_type_table(df.loc[:, ['id', 'engine_types']])


if __name__ == '__main__':
    main()
