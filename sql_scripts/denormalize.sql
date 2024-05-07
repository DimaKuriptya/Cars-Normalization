IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'denormalized')
BEGIN
    EXEC('CREATE SCHEMA denormalized'); 
END

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'denormalized.Company')
                  AND type in (N'U'))
BEGIN
    CREATE TABLE [denormalized].[Company] (
        brand NVARCHAR(80) NOT NULL,
        models TEXT NOT NULL,
        ceo NVARCHAR(120) NOT NULL,
        country NVARCHAR(80) NOT NULL,
        headquarters TEXT NOT NULL,
        engine_types TEXT NOT NULL,
        founders TEXT NOT NULL,
        foundation CHAR(10) NOT NULL,
        ev CHAR(1) NOT NULL,
        operating_income TEXT NOT NULL, 
    )
END