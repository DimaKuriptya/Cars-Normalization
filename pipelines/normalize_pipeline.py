import pathlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.normalize_etl import prepare_database, extract_from_excel, transform_data, load_data



def normalize_pipeline() -> None:
    prepare_database()

    url = pathlib.Path(__file__).parent.parent / 'xlsx_files/Table for Normalization.xlsx'
    df = extract_from_excel(url)

    transformed_dfs = transform_data(df)

    load_data(transformed_dfs)


if __name__ == '__main__':
    normalize_pipeline()
