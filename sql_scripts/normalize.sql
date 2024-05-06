IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'normalized')
BEGIN
    EXEC('CREATE SCHEMA normalized'); 
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.Country')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[Country] (
        id INT IDENTITY(1, 1),
        iso CHAR(2) NOT NULL,
        name NVARCHAR(80) NOT NULL,
        nicename NVARCHAR(80) NOT NULL,
        iso3 CHAR(3) NULL,
        numcode SMALLINT DEFAULT NULL,
        phonecode INT NOT NULL,
        CONSTRAINT PK_COUNTRY PRIMARY KEY (id),
    )
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.Company')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[Company] (
        id INT IDENTITY(1, 1),
        brand NVARCHAR(80) NOT NULL,
        ceo NVARCHAR(100) NOT NULL,
        country_id INT NOT NULL,
        foundation DATE NOT NULL,
        ev BIT NOT NULL,
        CONSTRAINT PK_COMPANY PRIMARY KEY (id),
        CONSTRAINT FK_COMPANY_REFERENCE_COUNTRY FOREIGN KEY (country_id) REFERENCES [normalized].[Country] (id),
    )
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.Model')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[Model] (
        id INT IDENTITY(1, 1),
        company_id INT NOT NULL,
        name NVARCHAR(80) NOT NULL,
        CONSTRAINT PK_MODEL PRIMARY KEY (id),
        CONSTRAINT FK_MODEL_REFERENCE_COMPANY FOREIGN KEY (company_id) REFERENCES [normalized].[Company] (id),
    )
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.Headquarter')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[Headquarter] (
        id INT IDENTITY(1, 1),
        company_id INT NOT NULL,
        country_id INT NOT NULL,
        city NVARCHAR(100) NOT NULL,
        CONSTRAINT PK_HEADQUARTER PRIMARY KEY (id),
        CONSTRAINT FK_HEADQUARTER_REFERENCE_COMPANY FOREIGN KEY (company_id) REFERENCES [normalized].[Company] (id),
        CONSTRAINT FK_HEADQUARTER_REFERENCE_COUNTRY FOREIGN KEY (country_id) REFERENCES [normalized].[Country] (id),
    )
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.Engine')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[Engine] (
        id INT IDENTITY(1, 1),
        type NVARCHAR(80) NOT NULL,
        CONSTRAINT PK_ENGINE PRIMARY KEY (id),
    )
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.EngineType')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[EngineType] (
        company_id INT,
        engine_id INT,
        CONSTRAINT PK_ENGINE_TYPE PRIMARY KEY (company_id, engine_id),
        CONSTRAINT FK_ENGINE_TYPE_REFERENCE_COMPANY FOREIGN KEY (company_id) REFERENCES [normalized].[Company] (id),
        CONSTRAINT FK_ENGINE_TYPE_REFERENCE_ENGINE FOREIGN KEY (engine_id) REFERENCES [normalized].[Engine] (id),
    )
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.Founder')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[Founder] (
        id INT IDENTITY(1, 1),
        company_id INT NOT NULL,
        CONSTRAINT PK_FOUNDER PRIMARY KEY (id),
        CONSTRAINT FK_FOUNDER_REFERENCE_COMPANY FOREIGN KEY (company_id) REFERENCES [normalized].[Company] (id),
    )
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'normalized.OperatingIncome')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [normalized].[OperatingIncome] (
        company_id INT,
        year INT,
        income INT NOT NULL
        CONSTRAINT PK_ENGINETYPE PRIMARY KEY (company_id, year),
        CONSTRAINT FK_OPERATING_INCOME_REFERENCE_COMPANY FOREIGN KEY (company_id) REFERENCES [normalized].[Company] (id),
    )
END
GO