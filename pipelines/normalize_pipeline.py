import pathlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.normalize_etl import prepare_database, extract_from_excel, transform_data, load_data
from config.settings import engine


def normalize_pipeline() -> None:
    prepare_database(engine)

    with engine.begin() as conn:
        url = pathlib.Path(__file__).parent.parent / 'xlsx_files/Table for Normalization.xlsx'
        df = extract_from_excel(url)

        transformed_dfs = transform_data(conn, df)

        load_data(conn, transformed_dfs)

        conn.commit()


if __name__ == '__main__':
    normalize_pipeline()
