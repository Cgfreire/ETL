--Consulta para obter o preço de fechamento de um ativo em uma data específica:
SELECT 
    da.Ativo,
    dd.DataReferencia,
    fc.PRECO_FECHAMENTO
FROM 
    dw.FactCotacao fc
JOIN 
    dw.DimAtivo da ON fc.IdDimAtivo = da.IdDimAtivo
JOIN 
    dw.DimData dd ON fc.IdDimData = dd.IdDimData
WHERE 
    da.Ativo = 'PETR4'  -- Substitua por um ativo desejado
    AND dd.DataReferencia = '2024-11-08';  -- Substitua por uma data desejada

--Consulta para obter o maior preço de fechamento de cada ativo no período:
SELECT 
    da.Ativo,
    MAX(fc.PRECO_FECHAMENTO) AS MaiorPrecoFechamento
FROM 
    dw.FactCotacao fc
JOIN 
    dw.DimAtivo da ON fc.IdDimAtivo = da.IdDimAtivo
GROUP BY 
    da.Ativo;

--Consulta para obter o volume de negociações de um ativo em um determinado período de tempo:
SELECT 
    da.Ativo,
    SUM(fc.VOLUME_NEGOCIACOES) AS VolumeNegociado
FROM 
    dw.FactCotacao fc
JOIN 
    dw.DimAtivo da ON fc.IdDimAtivo = da.IdDimAtivo
JOIN 
    dw.DimData dd ON fc.IdDimData = dd.IdDimData
WHERE 
    da.Ativo = 'PETR4'  -- Substitua por um ativo desejado
    AND dd.DataReferencia BETWEEN '2024-10-01' AND '2024-10-31'  -- Substitua por seu período desejado
GROUP BY 
    da.Ativo;


--Consulta para obter o preço médio de fechamento por ativo no último mês:
SELECT 
    da.Ativo,
    AVG(fc.PRECO_FECHAMENTO) AS PrecoMedioFechamento
FROM 
    dw.FactCotacao fc
JOIN 
    dw.DimAtivo da ON fc.IdDimAtivo = da.IdDimAtivo
JOIN 
    dw.DimData dd ON fc.IdDimData = dd.IdDimData
WHERE 
    dd.DataReferencia BETWEEN '2024-10-01' AND '2024-10-31'  -- Substitua com o último mês desejado
GROUP BY 
    da.Ativo;

-- Consulta para obter o preço máximo e mínimo de cada ativo em um período específico:
SELECT 
    da.Ativo,
    MAX(fc.PRECO_MAXIMO) AS PrecoMaximo,
    MIN(fc.PRECO_MINIMO) AS PrecoMinimo
FROM 
    dw.FactCotacao fc
JOIN 
    dw.DimAtivo da ON fc.IdDimAtivo = da.IdDimAtivo
JOIN 
    dw.DimData dd ON fc.IdDimData = dd.IdDimData
WHERE 
    dd.DataReferencia BETWEEN '2024-09-01' AND '2024-09-30'  -- Substitua pelo período desejado
GROUP BY 
    da.Ativo;
