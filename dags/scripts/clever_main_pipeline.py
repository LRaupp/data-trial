import pandas as pd
from scripts.postgres_helper import upload_overwrite_table

# This should resolve the issues presented and maintain the generic functionality of the function.
def upload_to_postgres(**kwargs):
    file_name=kwargs.get('file_name')
    table_name = file_name.split('.')[0]

    raw_df = pd.read_csv(
        f'dags/scripts/data_examples/{file_name}',
        header=0,
        sep=',',
        quotechar=  '"',
        escapechar="\\"
    )

    upload_overwrite_table(raw_df, table_name)
