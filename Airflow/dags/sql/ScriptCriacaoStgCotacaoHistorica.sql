CREATE TABLE [stg].[COTHIST](
	[PRECO_ABER] [varchar](max) NULL,
	[PRECO_MAX] [varchar](max) NULL,
	[PRECO_MIN] [varchar](max) NULL,
	[PRECO_MED] [varchar](max) NULL,
	[PRECO_ULT] [varchar](max) NULL,
	[PRECO_OFC] [varchar](max) NULL,
	[PRECO_OFV] [varchar](max) NULL,
	[QTD_TOTAL_NEGOCIOS] [varchar](max) NULL,
	[QTD_TOTAL_TITULOS_NEG] [varchar](max) NULL,
	[VOLUME_TOTAL_NEG] [varchar](max) NULL,
	[NOME_EMPRESA] [varchar](max) NULL,
	[DATA] [varchar](max) NULL,
	[ATIVO] [varchar](max) NULL
)


CREATE TABLE [dw].[FactIndicadores](
	[IdFactIndicadores] [int] IDENTITY(1,1) NOT NULL,
	[IdDimData] [int] NOT NULL,
	[IdDimAtivo] [int] NOT NULL,
	[IdDimEmpresa] [int] NOT NULL,
	[PRECO_ABERTURA] [numeric](15, 2) NULL,
	[PRECO_MAXIMO] [numeric](15, 2) NULL,
	[PRECO_MINIMO] [numeric](15, 2) NULL,
	[PRECO_MEDIO] [numeric](15, 2) NULL,
	[PRECO_ULTIMA_NEGOCIACAO] [numeric](15, 2) NULL,
	[PRECO_MELHOR_OFERTA_COMPRA] [numeric](15, 2) NULL,
	[PRECO_MELHOR_OFERTA_VENDA] [numeric](15, 2) NULL,
	[QTD_TOTAL_NEGOCIOS] [bigint] NULL,
	[QTD_TOTAL_TITULOS_NEGOCIADOS] [bigint] NULL,
	[VOLUME_TOTAL_TITULOS_NEGOCIADOS] [bigint] NULL
	)

ALTER TABLE dw.FactIndicadores ADD CONSTRAINT FK_FactIndicadores_DimAtivo FOREIGN KEY (IdDimAtivo) REFERENCES dw.DimAtivo(IdDimAtivo)
ALTER TABLE dw.FactIndicadores ADD CONSTRAINT FK_FactIndicadores_DimEmpresa FOREIGN KEY (IdDimEmpresa) REFERENCES dw.DimEmpresa(IdDimEmpresa)
ALTER TABLE dw.FactIndicadores ADD CONSTRAINT FK_FactIndicadores_DimData FOREIGN KEY (IdDimData) REFERENCES dw.DimData(IdDimData)

