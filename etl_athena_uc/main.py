from athena_mvsh import ( 
    Athena,
    CursorParquetDuckdb
)
from storage_data import SConnect, Storage
import os
from functools import wraps
from time import perf_counter
from models import data
import duckdb
from google.oauth2 import service_account
import io
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import json
from secret import access_secret_version


aws_access_key_id = os.getenv('aws_access_key_id', None)
aws_secret_access_key = os.getenv('aws_secret_access_key', None)
region_name = os.getenv('region_name', None)
s3_staging_dir = os.getenv('s3_staging_dir', None)
secret_id = os.getenv('secret_id', None)
project_id = os.getenv('project_id', None)
plan_base_coord = os.getenv('plan_base_coord', None)
bucket_to = os.getenv('bucket_to', None)
database_name = os.getenv('database_name', None)


JSON_DICT = json.loads(
    access_secret_version(
        project_id=project_id,
        secret_id=secret_id,
        version_id=1
    )
)


def tempo(func):
    @wraps(func)
    def inner(*args, **kwargs):
        t_01 = perf_counter()
        rst = func(*args, **kwargs)
        diff = perf_counter() - t_01
        print(f'Time: {diff:.8f}')
        return rst
    return inner


def download_file_drive(id_file, name_file):

    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    creds = service_account.Credentials.from_service_account_info(
        info=JSON_DICT,
        scopes=SCOPES
    )

    try:
        service = build('drive', 'v3', credentials=creds)
        request = service.files().get_media(fileId=id_file)

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)

        done = False

        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}.")

    except HttpError as e:
        print(f"An error occurred: {e}")
        file = None

    else:
        with open(name_file, 'wb') as f:
            f.write(file.getvalue())
        
        return name_file


def upload_file(bucket_name, file, to_file, sub_path = None):
    config = SConnect()
    cliente = config(project=project_id)

    storage =  Storage(cliente=cliente)
    bucket = storage.get_bucket(bucket_name)
    
    if sub_path:
       to_file = f'{sub_path}/{to_file}'

    storage.upload_large_file(bucket, file, to_file)


def etl_columns_select(
    cursor: CursorParquetDuckdb, 
    catalog_name: str, 
    database_name: str,
    table_name: str
) -> str:
    
    metadados = cursor.get_table_metadata(
        catalog_name=catalog_name,
        database_name=database_name,
        table_name=table_name
    )

    columns = map(
        lambda row: 
        f'cast({row[0]} as timestamp(3)) as {row[0]}' 
        if 'timestamp' in row else row[0],
        [tuple(row.values()) for row in  metadados['Columns']]
    )

    cols = '\n,'.join(columns)

    return f"""select {cols} from {database_name}.{table_name}"""


def transform_supervisor() -> None: 
    origem = download_file_drive(
        plan_base_coord, 
        'base_coord.xlsx'
    )
    
    with duckdb.connect('db.duckdb') as con:
        con.install_extension('spatial')
        con.load_extension('spatial')
    
        con.sql(rf"""
            CREATE OR REPLACE TABLE supervisor AS
            SELECT
                Filial AS fili_cd_filial,
                STRIP_ACCENTS(TRIM(SUPERVISOR)) AS supervisor
            FROM st_read(
                '{origem}',
                layer = 'Base',
                open_options = ['HEADERS=FORCE']
                )
                WHERE fili_cd_filial IS NOT null;
        """)

    os.unlink(origem)
    print('Base coord SUCESSO !')


def to_parquet(table_name: str) -> str:
    file_to = f'{table_name}.parquet'

    with duckdb.connect('db.duckdb') as con:
        transform_barra(con, table_name)
        tbl = con.table(table_name)
        tbl.write_parquet(file_to, row_group_size=100_000)  
    
    return file_to


def transform_barra(
    con: duckdb.DuckDBPyConnection, 
    table_name: str
) -> None:
 
    if any(
        [
            table_name == 'cosmos_v14b_dbo_ultima_chance_autorizacao',
            table_name == 'cosmos_v14b_dbo_ultima_chance_kardex'
        ]
    ):

        con.sql(f"""
            UPDATE {table_name}
                SET ulch_cd_barras = LPAD(TRIM(ulch_cd_barras), 30, '0')
        """)


def finalize_base(origem, destino):
    """
    Envia novo banco
    """
    transform_supervisor()
    upload_file(bucket_to, origem, destino)
    os.unlink(origem)

    print('data.duckdb SUCESSO !')


@tempo
def main_export_table(
    bucket, 
    sub_path, 
    table, 
    stmt,
    table_name
) -> None:
    
    cursor = CursorParquetDuckdb(
        s3_staging_dir,
        result_reuse_enable=True,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    
    with Athena(cursor=cursor) as cliente:
        if table:
            stmt_query = etl_columns_select(
                cliente.cursor,
                catalog_name='awsdatacatalog',
                database_name=database_name,
                table_name=stmt
            )

        else:
            stmt_query = stmt

        cliente.execute(stmt_query)
        cliente.to_create_table_db(table_name)

        to_file = to_parquet(table_name)
        upload_file(bucket, to_file, to_file, sub_path)
        os.unlink(to_file)


def export_tables(request):
    t_01 = perf_counter()

    for row in data:
        main_export_table(**row)

    finalize_base('db.duckdb', 'data.duckdb')

    diff = t_01 - perf_counter()
    retorno = f'Tempo total: {diff:.8f}'
    print(retorno)

    return retorno