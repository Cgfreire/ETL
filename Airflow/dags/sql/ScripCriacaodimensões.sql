-- Remover as tabelas se existirem
DROP TABLE IF EXISTS dw.FactCotacao CASCADE;
DROP TABLE IF EXISTS stg.COTHIST CASCADE;
DROP TABLE IF EXISTS dw.DimAtivo CASCADE;
DROP TABLE IF EXISTS dw.DimData CASCADE;

-- Criação da tabela dw.DimAtivo
CREATE TABLE dw.DimAtivo (
    IdDimAtivo SERIAL PRIMARY KEY,
    Ativo VARCHAR(20) NOT NULL
);

-- Criação da tabela dw.DimData
CREATE TABLE dw.DimData (
    IdDimData SERIAL PRIMARY KEY,
    DataReferencia DATE NOT NULL
);
