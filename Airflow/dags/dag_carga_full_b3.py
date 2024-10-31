import datetime
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


#DimAtivo
def sql_insert_dim_ativo(ativo):
    return f"""INSERT INTO dw.DimAtivo (Ativo) VALUES ('{ativo}')"""

#DimData
def sql_insert_dim_data(data):
  return f"""INSERT INTO dw.DimData (DataReferencia) VALUES ('{data}')"""

#DimEmpresa
def sql_insert_dim_empresa(empresa):
  return f"""INSERT INTO dw.DimEmpresa (Empresa) VALUES ('{empresa}')"""

#FactIndicadores
def sql_insert_dim_factIndicadores(PRECO_ABER,PRECO_MAX,PRECO_MIN,PRECO_MED,PRECO_ULT,PRECO_OFC,PRECO_OFV,QTD_TOTAL_NEGOCIOS,QTD_TOTAL_TITULOS_NEG,VOLUME_TOTAL_NEG, NOME_EMPRESA,DATA,ATIVO):
  return f"""INSERT INTO stg.COTHIST (PRECO_ABER,PRECO_MAX,PRECO_MIN,PRECO_MED,PRECO_ULT,PRECO_OFC,PRECO_OFV,QTD_TOTAL_NEGOCIOS,QTD_TOTAL_TITULOS_NEG,VOLUME_TOTAL_NEG, NOME_EMPRESA,DATA,ATIVO) VALUES
  ({PRECO_ABER},{PRECO_MAX},{PRECO_MIN},{PRECO_MED},{PRECO_ULT},{PRECO_OFC},{PRECO_OFV},{QTD_TOTAL_NEGOCIOS},{QTD_TOTAL_TITULOS_NEG},{VOLUME_TOTAL_NEG},'{NOME_EMPRESA}',{DATA},'{ATIVO}')"""


def carga_full():
  pg_hook = PostgresHook('postgrees-airflow')
  conn = pg_hook.get_conn()
  cursor = conn.cursor()
  cabecalhos = [
  'TIPO','DATA','CODBDI','ATIVO',
  'TPMERC','NOME_EMPRESA','ESPEC_ATIVO','MOEDAREF',
  'PRECO_ABER','PRECO_MAX','PRECO_MIN','PRECO_MED',
  'PRECO_ULT','PRECO_OFC','PRECO_OFV','QTD_TOTAL_NEGOCIOS',
  'QTD_TOTAL_TITULOS_NEG','VOLUME_TOTAL_NEG',
  'PREXE','INDOPC','DATA_VEN','FATCOT','PTOEXE','CODSI','DISMES']
  df = pd.read_csv('dags/files/COTAHIST_A2024.TXT', sep=';', names=cabecalhos, skiprows = 1, index_col = False)

  df_colunascorrigidas = df.copy()
  for i in range(7, len(cabecalhos) - 1):
      df_colunascorrigidas.iloc[:, i] = df.iloc[:, i+1]

  lista_ativos_selecionados = ['VALE3','CMIG4','ITSA4','KLBN4','VIVT3','SAPR4','TAEE4','AZUL4','PETZ3']
  df_v2 = df_colunascorrigidas.copy()
  df_v2 = df_v2[df_v2['ATIVO'].isin(lista_ativos_selecionados)]


  lista_insere_ativos = df_v2['ATIVO'].unique().tolist()
  for i in range(0,len(lista_insere_ativos)):
    cursor.execute(sql_insert_dim_ativo(lista_insere_ativos[i]))
      

  lista_insere_datas = df_v2['DATA'].unique().tolist()
  for i in range(0,len(lista_insere_datas)):
    cursor.execute(sql_insert_dim_data(lista_insere_datas[i]))
      

  lista_insere_empresa = df_v2['NOME_EMPRESA'].unique().tolist()
  for i in range(0,len(lista_insere_empresa)):
    cursor.execute(sql_insert_dim_empresa(lista_insere_empresa[i]))
      

  FactIndicadores = df_v2[['PRECO_ABER', 'PRECO_MAX','PRECO_MIN','PRECO_MED','PRECO_ULT','PRECO_OFC','PRECO_OFV','QTD_TOTAL_NEGOCIOS','QTD_TOTAL_TITULOS_NEG','VOLUME_TOTAL_NEG','NOME_EMPRESA','DATA','ATIVO']]
  for i in range(0,len(FactIndicadores)):
    cursor.execute(sql_insert_dim_factIndicadores(FactIndicadores['PRECO_ABER'].iloc[i],FactIndicadores['PRECO_MAX'].iloc[i],FactIndicadores['PRECO_MIN'].iloc[i]
                                  ,FactIndicadores['PRECO_MED'].iloc[i],FactIndicadores['PRECO_ULT'].iloc[i]
                                  ,FactIndicadores['PRECO_OFC'].iloc[i],FactIndicadores['PRECO_OFV'].iloc[i],FactIndicadores['QTD_TOTAL_NEGOCIOS'].iloc[i]
                                  ,FactIndicadores['QTD_TOTAL_TITULOS_NEG'].iloc[i],FactIndicadores['VOLUME_TOTAL_NEG'].iloc[i]
                                  ,FactIndicadores['NOME_EMPRESA'].iloc[i],FactIndicadores['DATA'].iloc[i],FactIndicadores['ATIVO'].iloc[i]))
  
  
with DAG(dag_id ='carga_full_b3',start_date=datetime.datetime(2024, 10, 31), schedule="@hourly", template_searchpath = 'dags/sql') as dag:
    carga_full_b3 = PythonOperator(task_id = 'carga_full_b3', python_callable = carga_full)
    cria_dimensoes = PostgresOperator(task_id = 'cria_dimensoes',postgres_conn_id = 'postgrees-airflow', sql = 'ScripCriacaodimensões.sql')
    cria_fatos = PostgresOperator(task_id = 'cria_fatos',postgres_conn_id = 'postgrees-airflow', sql = 'ScriptCriacaoFato.sql')

    cria_dimensoes >> cria_fatos >> carga_full_b3