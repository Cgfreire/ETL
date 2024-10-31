--DimAtivo

CREATE TABLE dw.DimAtivo (
	IdDimAtivo INT IDENTITY,
	Ativo VARCHAR(20) NOT NULL,
)
ALTER TABLE dw.DimAtivo ADD CONSTRAINT PK_DimAtivo_IdDimAtivo PRIMARY KEY (IdDimAtivo)

--DimEmpresa
CREATE TABLE dw.DimEmpresa (
	IdDimEmpresa INT IDENTITY,
	Empresa VARCHAR(20) NOT NULL,
)
ALTER TABLE dw.DimEmpresa ADD CONSTRAINT PK_DimEmpresa_IdDimEmpresa PRIMARY KEY (IdDimEmpresa)

--DimData
CREATE TABLE dw.DimData (
	IdDimData INT IDENTITY,
	DataReferencia VARCHAR(20) NOT NULL,
)
ALTER TABLE dw.DimData ADD CONSTRAINT PK_DimData_IdDimData PRIMARY KEY (IdDimData)


