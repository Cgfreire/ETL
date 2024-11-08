DROP TABLE IF EXISTS stg.COTHIST CASCADE;

CREATE TABLE stg.COTHIST (
    tipo_registro TEXT,
    data_pregao TEXT,
    cod_bdi TEXT,
    cod_negociacao TEXT,
    tipo_mercado TEXT,
    nome_empresa TEXT,
    moeda TEXT,
    preco_abertura TEXT,
    preco_maximo TEXT,
    preco_minimo TEXT,
    preco_medio TEXT,
    preco_ultimo_negocio TEXT ,
    preco_melhor_oferta_compra TEXT,
    preco_melhor_oferta_venda TEXT,
    numero_negocios TEXT,
    quantidade_papeis_negociados TEXT,
    volume_total_negociado TEXT,
    codigo_isin TEXT, 
    num_distribuicao_papel TEXT
);