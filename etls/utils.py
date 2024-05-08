import pandas as pd
from pandas.errors import DatabaseError, MergeError


class Loader:
    """
    Class is used to load df data to the database
    """
    def __init__(self, engine, schema) -> None:
        self.engine = engine
        self.schema = schema

    def load_to_database(self, df: pd.DataFrame, table_name: str) -> None:
        try:
            df.to_sql(table_name, schema=self.schema, con=self.engine, if_exists='append', index=False)
        except DatabaseError as e:
            print(f'Unable to load the dataframe into {table_name} table: {e}')


class Normalizer:
    """
    Class is used to transform and normalize df data
    """
    def __init__(self, engine) -> None:
        self.engine = engine

    def transform_company_df(self, df: pd.DataFrame) -> None:
        country_df = pd.read_sql(sql='SELECT id, nicename FROM [normalized].[Country]', con=self.engine)
        try:
            df = df.merge(country_df, left_on='country', right_on='nicename', how='left')
        except MergeError as e:
            print('Unable to merge: ' + e)

        df = df.rename(columns={'id_y': 'country_id'})
        df = df.drop(columns=['country', 'nicename', 'id_x'])
        return df

    def transform_engine_type_df(self, df: pd.DataFrame) -> None:
        try:
            engine_df = pd.read_sql(sql="SELECT id, type FROM normalized.Engine", con=self.engine)
        except DatabaseError as e:
            print('Unable to get engine types: ' + e)

        engine_type_data = []

        for _, row in df.iterrows():
            for engine_type in row['engine_types']:
                engine_id = engine_df.loc[engine_df['type'] == engine_type, 'id'].values[0]
                engine_type_data.append({'company_id': row['id'], 'engine_id': engine_id})

        engine_type_df = pd.DataFrame(engine_type_data)
        return engine_type_df

    def transform_model_df(self, df: pd.DataFrame) -> None:
        models_data = []

        for _, row in df.iterrows():
            for model in row['models']:
                models_data.append({'company_id': row['id'], 'name': model})

        model_df = pd.DataFrame(models_data)
        return model_df

    def transform_founder_df(self, df: pd.DataFrame) -> None:
        founders_data = []

        for _, row in df.iterrows():
            for full_name in row['founders']:
                founders_data.append({'company_id': row['id'], 'full_name': full_name})

        founder_df = pd.DataFrame(founders_data)
        return founder_df

    def transform_operating_income_df(self, df: pd.DataFrame) -> None:
        income_data = []

        for _, row in df.iterrows():
            for yearly_info in row['operating_income']:
                yearly_info = yearly_info.split(': ')
                income_data.append({'company_id': row['id'], 'year': int(yearly_info[0]), 'income': int(yearly_info[1])})

        operating_income_df = pd.DataFrame(income_data)
        return operating_income_df

    def transform_headquarter_df(self, df: pd.DataFrame) -> None:
        headquarters_data = []

        try:
            country_df = pd.read_sql(sql="SELECT id, nicename FROM normalized.Country", con=self.engine)
        except DatabaseError as e:
            print('Unable to get countries: ' + e)

        for _, row in df.iterrows():
            row['headquarters'][-1] = country_df.loc[country_df['nicename'] == row['headquarters'][-1], 'id'].values[0] # Transforming str country to foreign key
            headquarters_data.append({'company_id': row['id'], 'country_id': row['headquarters'][-1], 'city': row['headquarters'][0]})

        headquarters_df = pd.DataFrame(headquarters_data)
        return headquarters_df
