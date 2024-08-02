

data = [
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_assist_ger_regional', 'table_name': 'cosmos_v14b_dbo_assist_ger_regional'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_cargo', 'table_name': 'cosmos_v14b_dbo_cargo'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_categ_prd_ephcab', 'table_name': 'cosmos_v14b_dbo_categ_prd_ephcab'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_categ_prd_ephdet', 'table_name': 'cosmos_v14b_dbo_categ_prd_ephdet'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_categoria_produto_novo', 'table_name': 'cosmos_v14b_dbo_categoria_produto_novo'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_deposito', 'table_name': 'cosmos_v14b_dbo_deposito'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_estado', 'table_name': 'cosmos_v14b_dbo_estado'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_filial', 'table_name': 'cosmos_v14b_dbo_filial'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_fornecedor', 'table_name': 'cosmos_v14b_dbo_fornecedor'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_gerente_operacao', 'table_name': 'cosmos_v14b_dbo_gerente_operacao'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_gerente_regional', 'table_name': 'cosmos_v14b_dbo_gerente_regional'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_grupo_compra', 'table_name': 'cosmos_v14b_dbo_grupo_compra'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_municipio', 'table_name': 'cosmos_v14b_dbo_municipio'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_produto_deposito', 'table_name': 'cosmos_v14b_dbo_produto_deposito'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_produto_mestre', 'table_name': 'cosmos_v14b_dbo_produto_mestre'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': True, 'stmt': 'cosmos_v14b_dbo_usuario', 'table_name': 'cosmos_v14b_dbo_usuario'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'DIMENSIONS', 'table': False, 'stmt': """SELECT fili_cd_filial, prme_cd_produto, prfi_qt_estoqatual, prfi_qt_estindisp, prfi_vl_cmpcsicms, prfi_tp_clabcfat, prfi_tp_sclabcfat from modelled.cosmos_v14b_dbo_produto_filial""", 'table_name': 'cosmos_v14b_dbo_produto_filial'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'ULTIMA_CHANCE', 'table': True, 'stmt': 'cosmos_v14b_dbo_ultima_chance_autorizacao', 'table_name': 'cosmos_v14b_dbo_ultima_chance_autorizacao'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'ULTIMA_CHANCE', 'table': True, 'stmt': 'cosmos_v14b_dbo_ultima_chance_kardex', 'table_name': 'cosmos_v14b_dbo_ultima_chance_kardex'},
    {'bucket': 'datalake-mvsh', 'sub_path': 'ULTIMA_CHANCE', 'table': True, 'stmt': 'cosmos_v14b_dbo_ultima_chance_produto', 'table_name': 'cosmos_v14b_dbo_ultima_chance_produto'},
]