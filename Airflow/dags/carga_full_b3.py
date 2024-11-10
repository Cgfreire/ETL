from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import logging, os

def get_db_connection():
    hook = PostgresHook(postgres_conn_id='postgrees-airflow')
    return hook.get_conn()

def carrega_stg(data_chunk):
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

        for record in data_chunk:
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
        logging.info("Chunk inserido com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao inserir chunk: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def extracao():
    anos = ['2022', '2023', '2024']
    arquivos = [f'dags/files/COTAHIST_A{ano}.txt' for ano in anos]
        
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
        for arquivo in arquivos:
            logging.info("Processando arquivo: %s", arquivo)
            
            if not os.path.isfile(arquivo):
                logging.error("Arquivo não encontrado: %s", arquivo)
                continue
            
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
        logging.error("Erro na extração: %s", e)

def carrega_dim_data(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO dw.DimData (DataReferencia)
        SELECT DISTINCT
            CASE 
                WHEN data_pregao ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(data_pregao, 'DD/MM/YYYY')
                ELSE TO_dATE('31-12-9999', 'DD/MM/YYYY')
            END AS data
        FROM stg.COTHIST
        WHERE data_pregao IS NOT NULL AND data_pregao <> ''
        """)
        conn.commit()

def carrega_dw():
    conn = get_db_connection()
    
    try:
        carrega_dim_data(conn)

        with conn.cursor() as cursor:
            cursor.execute(""" 
            INSERT INTO dw.DimAtivo (ativo) 
            SELECT DISTINCT cod_negociacao FROM stg.COTHIST 
            """)

            cursor.execute(""" 
            INSERT INTO dw.FactCotacao (IdDimAtivo, IdDimData, PRECO_ABERTURA, PRECO_FECHAMENTO, PRECO_MAXIMO, PRECO_MINIMO, VOLUME_NEGOCIACOES) 
            SELECT 
                da.idDimAtivo, 
                dc.idDimData, 
                sc.preco_abertura::numeric, 
                sc.preco_ultimo_negocio::numeric,
                sc.preco_maximo::numeric, 
                sc.preco_minimo::numeric, 
                sc.volume_total_negociado::numeric
			FROM stg.COTHIST sc
			LEFT JOIN dw.DimAtivo da ON sc.cod_negociacao = da.ativo
			LEFT JOIN dw.DimData dc ON 
			  (CASE WHEN data_pregao ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(data_pregao, 'DD/MM/YYYY')
			       ELSE TO_DATE('31-12-9999', 'DD/MM/YYYY')
			  END) = dc.DataREferencia;
            """)

        conn.commit()

    except Exception as e:
        conn.rollback()

    finally:
        conn.close()

with DAG('carga_full_b3', schedule_interval='@monthly', start_date=datetime(2024, 1, 1), catchup=False,template_searchpath = 'dags/sql') as dag:
    cria_tabelas_dim_task = PostgresOperator(task_id = 'cria_tabelas_dim', postgres_conn_id = 'postgrees-airflow', sql='ScripCriacaodimensões.sql')
    cria_tabelas_fact_task = PostgresOperator(task_id = 'cria_tabelas_fact',postgres_conn_id = 'postgrees-airflow', sql='ScriptCriacaoFato.sql')
    extracao_task = PythonOperator(task_id='extracao', python_callable=extracao)
    carrega_dw_task = PythonOperator(task_id='carrega_dw', python_callable=carrega_dw)

    cria_tabelas_dim_task >> cria_tabelas_fact_task >> extracao_task >> carrega_dw_task