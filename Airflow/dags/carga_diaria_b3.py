from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd


def get_db_connection():
    """Obtém uma conexão com o banco de dados PostgreSQL."""
    hook = PostgresHook(postgres_conn_id='postgrees-airflow')
    return hook.get_conn()

def extracao():
    arquivo = f'/opt/airflow/files/COTAHIST_A{datetime.date.today().year}.txt'
        
    separar_campos = [2, 8, 2, 12, 3, 12, 10, 3, 4, 13, 13, 13, 13, 13, 13, 13, 5, 18, 18, 13, 1, 8, 7, 13, 12, 3]
    colunas = [
        "tipo_registro", "data_pregao", "cod_bdi", "cod_negociacao", "tipo_mercado", 
        "nome_empresa", "especificacao_papel", "prazo_dias_merc_termo", "moeda", 
        "preco_abertura", "preco_maximo", "preco_minimo", "preco_medio", 
        "preco_ultimo_negocio", "preco_melhor_oferta_compra", "preco_melhor_oferta_venda", 
        "numero_negocios", "quantidade_papeis_negociados", "volume_total_negociado", 
        "preco_exercicio", "indicador_correcao_precos", "data_vencimento", 
        "fator_cotacao", "preco_exercicio_pontos", "codigo_isin", "num_distribuicao_papel"
    ]

    chunk_size = 10000
    
    try:
        
        for chunk in pd.read_fwf(arquivo, widths=separar_campos, header=0, chunksize=chunk_size):
                        
            chunk.columns = colunas
            
            dinheiro = [
                "preco_abertura", "preco_maximo", "preco_minimo", 
                "preco_medio", "preco_ultimo_negocio", 
                "preco_melhor_oferta_compra", "preco_melhor_oferta_venda"
            ]

            for coluna in dinheiro:
                if coluna in chunk.columns:
                    chunk[coluna] = chunk[coluna].astype(float) / 100

            chunk['data_pregao'] = pd.to_datetime(chunk['data_pregao'], format='%Y%m%d', errors='coerce')
            chunk['data_pregao'] = chunk['data_pregao'].dt.strftime('%d/%m/%Y')

            carrega_stg(chunk.to_dict(orient='records'))

    except Exception as e:
        return []


def carrega_stg(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        insert_query = """
        INSERT INTO stg.COTHIST (
            tipo_registro,
            data_pregao,
            cod_bdi,
            cod_negociacao,
            tipo_mercado,
            nome_empresa,
            moeda,
            preco_abertura,
            preco_maximo,
            preco_minimo,
            preco_medio,
            preco_ultimo_negocio,
            preco_melhor_oferta_compra,
            preco_melhor_oferta_venda,
            numero_negocios,
            quantidade_papeis_negociados,
            volume_total_negociado,
            codigo_isin,
            num_distribuicao_papel
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        if data:
            for record in data:
                values = (
                    str(record['tipo_registro']),
                    str(record['data_pregao']),
                    str(record['cod_bdi']),
                    str(record['cod_negociacao']),
                    str(record['tipo_mercado']),
                    str(record['nome_empresa']),
                    str(record['moeda']),
                    float(record['preco_abertura']),
                    float(record['preco_maximo']),
                    float(record['preco_minimo']),
                    float(record['preco_medio']),
                    float(record['preco_ultimo_negocio']),
                    float(record['preco_melhor_oferta_compra']),
                    float(record['preco_melhor_oferta_venda']),
                    str(record['numero_negocios']),
                    str(record['quantidade_papeis_negociados']),
                    str(record['volume_total_negociado']),
                    str(record.get('codigo_isin', '')),
                    str(record.get('num_distribuicao_papel', ''))
                )

                cursor.execute(insert_query, values)
            
            conn.commit()


    except Exception as e:
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def carrega_dim_data(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO dw.DimData (DataReferencia)
        SELECT DISTINCT 
            TO_DATE(data_pregao, 'YYYYMMDD') AS data
        FROM stg.COTHIST
        LEFT JOIN dw.DimData b on TO_DATE(data_pregao, 'YYYYMMDD') b.DataReferencia
        WHERE data_pregao IS NOT NULL AND data_pregao <> '' and TO_DATE(data_pregao, 'YYYYMMDD') = GETDATE() and b.DataReferencia is NULL
        ON CONFLICT (DataReferencia) DO NOTHING;
        """)
        conn.commit()

def carrega_dw():
    conn = get_db_connection()
    
    try:
        carrega_dim_data(conn)

        with conn.cursor() as cursor:
            cursor.execute(""" 
            INSERT INTO dw.DimAtivo (ativo) 
            SELECT DISTINCT cod_negociacao FROM stg.COTHIST a LEFT JOIN dw.DimAtivo b on a.cod_negociacao <> b.Ativo
            WHERE b.Ativo is null
            ON CONFLICT (ativo) DO NOTHING;
            """)

            cursor.execute(""" 
           INSERT INTO dw.FactCotacao (IdDimAtivo, IdDimData, PRECO_ABERTURA, PRECO_FECHAMENTO, PRECO_MAXIMO, PRECO_MINIMO, VOLUME_NEGOCIACOES) 
            SELECT da.id, dc.id, sc.preco_abertura, sc.preco_ultimo_negocio,sc.preco_maximo, sc.preco_minimo, sc.volume_total_negociado 
            FROM stg.COTHIST sc
            JOIN dw.DimAtivo da ON sc.cod_negociacao = da.ativo
            JOIN dw.DimData dc ON TO_DATE(sc.data_pregao, 'YYYYMMDD') = dc.data
            WHERE TO_DATE(data_pregao, 'YYYYMMDD') = GETDATE();
            """)

        conn.commit()

    except Exception as e:
        conn.rollback()

    finally:
        conn.close()

with DAG('carga_diaria_b3', schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False,template_searchpath = 'dags/sql') as dag:
    cria_tabela_stage = PostgresOperator(task_id = 'cria_tabela_stage',postgres_conn_id = 'postgrees-airflow', sql='ScriptCriacaoStageDiaria.sql')
    extracao_task = PythonOperator(task_id='extracao', python_callable=extracao)
    carrega_stg_task = PythonOperator(task_id='carrega_stg',python_callable=carrega_stg,op_kwargs={'data': extracao_task.output})
    carrega_dw_task = PythonOperator(task_id='carrega_dw', python_callable=carrega_dw)

    cria_tabela_stage >> extracao_task >> carrega_stg_task >> carrega_dw_task