from datetime import datetime
import pathlib
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from settings import engine


def prepare_database() -> None:
    denormalize_url = pathlib.Path(__file__).parent / 'sql_scripts/denormalize.sql'

    with engine.connect() as con:
        with open(denormalize_url, encoding='utf-8') as file:
            query = text(file.read())
            con.execute(query)
            con.commit()


def merge_with_country_df(df: pd.DataFrame) -> pd.DataFrame:
    country_df = pd.read_sql(sql='SELECT id, nicename FROM [normalized].[Country]', con=engine)
    df = df.merge(country_df, left_on='country_id', right_on='id', how='left')
    df = df.rename(columns={'id_x': 'id', 'nicename': 'country'})

    return df.drop(columns=['country_id', 'id_y'])


def merge_with_company_df(company_df: pd.DataFrame, other_df: pd.DataFrame) -> pd.DataFrame:
    company_df = company_df.merge(other_df, left_on='id', right_on='company_id', how='left')
    company_df = company_df.rename(columns={'id_x': 'id'})

    return company_df.drop(columns=['company_id'])


def get_company_df() -> pd.DataFrame:
    normalized_company_df = pd.read_sql(sql='SELECT * FROM [normalized].[Company]', con=engine)
    company_df = merge_with_country_df(normalized_company_df)
    company_df['foundation'] = company_df['foundation'].apply(lambda x: x.strftime("%d.%m.%Y"))
    company_df['ev'] = company_df['ev'].apply(lambda x: 'Y' if True else 'N')

    return company_df


def get_model_df() -> pd.DataFrame:
    normalized_model_df = pd.read_sql(sql='SELECT * FROM [normalized].[Model]', con=engine)
    model_df = normalized_model_df.groupby('company_id')['name'].agg(lambda x: ', '.join(x)).reset_index()
    model_df = model_df.rename(columns={'name': 'models'})

    return model_df


def get_headquarter_df() -> pd.DataFrame:
    normalized_headquarter_df = pd.read_sql(sql='SELECT * FROM [normalized].[Headquarter]', con=engine)
    headquarter_df = merge_with_country_df(normalized_headquarter_df)
    headquarter_df['headquarters'] = headquarter_df['city'] + ", "  + headquarter_df['country']
    headquarter_df = headquarter_df.loc[:, ['company_id', 'headquarters']]

    return headquarter_df


def get_engine_type_df() -> pd.DataFrame:
    engine_df = pd.read_sql(sql='SELECT * FROM [normalized].[Engine]', con=engine)

    normalized_engine_type_df = pd.read_sql(sql='SELECT * FROM [normalized].[EngineType]', con=engine)
    engine_type_df = normalized_engine_type_df.merge(engine_df, left_on='engine_id', right_on='id', how='left')
    engine_type_df = engine_type_df.groupby('company_id')['type'].agg(lambda x: ', '.join(x)).reset_index()
    engine_type_df = engine_type_df.rename(columns={'type': 'engine_types'})

    return engine_type_df


def get_founder_df() -> pd.DataFrame:
    normalized_founder_df = pd.read_sql(sql='SELECT * FROM [normalized].[Founder]', con=engine)
    founder_df = normalized_founder_df.groupby('company_id')['full_name'].agg(lambda x: ',\n'.join(x)).reset_index()
    founder_df = founder_df.rename(columns={'full_name': 'founders'})

    return founder_df


def get_operating_income_df() -> pd.DataFrame:
    normalized_operating_income_df = pd.read_sql(sql='SELECT * FROM [normalized].[OperatingIncome]', con=engine)
    operating_income_df = normalized_operating_income_df.copy()
    operating_income_df['year'] = operating_income_df['year'].astype(str)
    operating_income_df['income'] = operating_income_df['income'].astype(str)
    operating_income_df['operating_income'] = operating_income_df["year"] + ': ' + operating_income_df["income"]
    operating_income_df = operating_income_df.groupby('company_id')['operating_income'].agg(lambda x: '\n'.join(x)).reset_index()

    return operating_income_df


def collect_data_into_single_df() -> pd.DataFrame:
    company_df = get_company_df()

    df_to_merge = {}
    df_to_merge['model_df'] = get_model_df()
    df_to_merge['headquarter_df'] = get_headquarter_df()
    df_to_merge['engine_type_df'] = get_engine_type_df()
    df_to_merge['founder_df'] = get_founder_df()
    df_to_merge['operating_income_df'] = get_operating_income_df()

    for value in df_to_merge.values():
        company_df = merge_with_company_df(company_df=company_df, other_df=value)

    company_df = company_df.drop(columns=['id'])

    return company_df


def load_denormalized_data(df):
    try:
        df.to_sql('Company', schema='denormalized', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(f'Unable to load the dataframe into Company table: {e}')


def main() -> None:
    prepare_database()
    df = collect_data_into_single_df()
    load_denormalized_data(df)


if __name__ == '__main__':
    main()
