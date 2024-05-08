import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.denormalize_etl import prepare_database, extract_data, transform_data, load_data


def denormalize_pipeline() -> None:
    prepare_database()

    company_df, df_list = extract_data()

    df = transform_data(company_df, df_list)

    load_data(df, table_name='Company')


if __name__ == '__main__':
    denormalize_pipeline()
