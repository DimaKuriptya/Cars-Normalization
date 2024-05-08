import pathlib
from datetime import datetime
import openpyxl
import pandas as pd
from sqlalchemy import text

from config.settings import engine
from etls.utils import Normalizer, Loader


def prepare_database() -> None:
    normalize_url = pathlib.Path(__file__).parent.parent / 'sql_scripts/normalize.sql'
    countries_url = pathlib.Path(__file__).parent.parent / 'sql_scripts/countries.sql'
    engines_url = pathlib.Path(__file__).parent.parent / 'sql_scripts/engines.sql'

    with engine.connect() as con:
        for path in [normalize_url, countries_url, engines_url]:
            with open(path, encoding='utf-8') as file:
                query = text(file.read())
                con.execute(query)
                con.commit()


def validate_date(raw_date: str | datetime) -> str:
    try:
        if isinstance(raw_date, str):
            date_obj = datetime.strptime(raw_date, "%d/%m/%Y")
        else:
            date_obj = raw_date

        ready_date = date_obj.strftime("%Y-%m-%d")
    except ValueError:
        print("Invalid date format")

    return ready_date


def fetch_excel_row(row: tuple, row_id: int) -> dict:
    data = {}
    data['id'] = row_id
    data['brand'] = row[0].value
    data['ceo'] = row[2].value.strip()
    data['country'] = row[3].value
    data['foundation'] = validate_date(row[7].value)
    data['ev'] = True if row[8].value == 'Y' else False
    data['models'] = row[1].value.split(', ')
    data['headquarters'] = row[4].value.rsplit(', ', 1)
    data['engine_types'] = row[5].value.split(', ')
    data['founders'] = list(map(lambda x: x.strip(), row[6].value.replace('\n', '').replace('\t', '').split(',')))
    data['operating_income'] = row[9].value.split('\n')

    return data


def extract_from_excel(url) -> pd.DataFrame:
    workbook = openpyxl.load_workbook(url)
    worksheet = workbook.active

    start_row = 5
    rows = worksheet.iter_rows(min_row=start_row)
    total_data = []

    for row_id, row in enumerate(rows):
        row_df = fetch_excel_row(row, row_id)
        total_data.append(row_df)

    df = pd.DataFrame(total_data)
    return df


def transform_data(df) -> dict:
    n = Normalizer(engine)
    transformed_df = {}

    transformed_df['Company'] = n.transform_company_df(df.loc[:, ['id', 'brand', 'ceo', 'country', 'foundation', 'ev']])
    transformed_df['OperatingIncome'] = n.transform_operating_income_df(df.loc[:, ['id', 'operating_income']])
    transformed_df['Model'] = n.transform_model_df(df.loc[:, ['id', 'models']])
    transformed_df['Founder'] = n.transform_founder_df(df.loc[:, ['id', 'founders']])
    transformed_df['Headquarter'] = n.transform_headquarter_df(df.loc[:, ['id', 'headquarters']])
    transformed_df['EngineType'] = n.transform_engine_type_df(df.loc[:, ['id', 'engine_types']])

    return transformed_df


def load_data(df_dict: dict[pd.DataFrame]) -> None:
    l = Loader(engine, schema='normalized')

    for table_name, df in df_dict.items():
        l.load_to_database(df, table_name)
