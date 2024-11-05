-- Criação da tabela stg.COTHIST
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

-- Criação da tabela dw.FactIndicadores
CREATE TABLE dw.FactCotacao (
    IdFactIndicadores SERIAL PRIMARY KEY,
    IdDimAtivo INT NOT NULL,
    IdDimData INT NOT NULL,
    PRECO_ABERTURA NUMERIC(15, 2),
    PRECO_FECHAMENTO NUMERIC(15, 2),
    VOLUME_NEGOCIACOES BIGINT
);

-- Adicionando as restrições de chave estrangeira
ALTER TABLE dw.FactIndicadores 
ADD CONSTRAINT FK_FactIndicadores_DimAtivo 
FOREIGN KEY (IdDimAtivo) REFERENCES dw.DimAtivo(IdDimAtivo);

ALTER TABLE dw.FactIndicadores 
ADD CONSTRAINT FK_FactIndicadores_DimData 
FOREIGN KEY (IdDimData) REFERENCES dw.DimData(IdDimData);
