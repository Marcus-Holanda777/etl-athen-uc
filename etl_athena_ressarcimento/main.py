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
import json
from secret import access_secret_version


aws_access_key_id = os.getenv('aws_access_key_id', None)
aws_secret_access_key = os.getenv('aws_secret_access_key', None)
region_name = os.getenv('region_name', None)
s3_staging_dir = os.getenv('s3_staging_dir', None)
secret_id = os.getenv('secret_id', None)
project_id = os.getenv('project_id', None)
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

    if table_name == 'cosmospdp_dbo_nota_cab':
        where = "where id_sistema_nota in(2, 3)"
        return f"""select {cols} from {database_name}.{table_name} {where}"""
    
    if table_name == 'cosmospdp_dbo_nota_det':
        where = "where id_sistema_nota in(2, 3)"
        return f"""
           select {cols} 
           from {database_name}.{table_name}
           inner join {database_name}.cosmospdp_dbo_nota_cab using(id_nota_cab)
           {where}
        """

    return f"""select {cols} from {database_name}.{table_name}"""


def to_parquet(table_name: str) -> str:
    file_to = f'{table_name}.parquet'

    with duckdb.connect('db.duckdb') as con:
        tbl = con.table(table_name)
        tbl.write_parquet(file_to, row_group_size=100_000)  
    
    return file_to


def finalize_base(origem, destino):
    """
    Envia novo banco
    """

    upload_file(bucket_to, origem, destino)
    os.unlink(origem)

    print('ressarcimento.duckdb SUCESSO !')


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


def export_ressarcimento(request):
    t_01 = perf_counter()

    for row in data:
        main_export_table(**row)

    finalize_base('db.duckdb', 'ressarcimento.duckdb')

    diff = perf_counter() - t_01
    retorno = f'Tempo total: {diff:.8f}'
    print(retorno)

    return retorno