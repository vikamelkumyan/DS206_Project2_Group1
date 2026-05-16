/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_suppliers.sql
    Purpose: Populate DimSuppliers from Suppliers.

    SCD logic: SCD4.
    Current values stay in DimSuppliers. Previous values are inserted into DimSuppliers_History.
    Parameters expected from Python .format():
        database_name
        schema_name
        source_table_name
        target_table_name
*/

SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @Today DATE = CAST(GETDATE() AS DATE);
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, @Today);
    DECLARE @SOR_SK INT;

    DECLARE @ChangedSuppliers TABLE (
        SupplierID_NK INT,
        CompanyName NVARCHAR(100),
        ContactName NVARCHAR(100),
        ContactTitle NVARCHAR(100),
        Address NVARCHAR(200),
        City NVARCHAR(100),
        Region NVARCHAR(100),
        PostalCode NVARCHAR(30),
        Country NVARCHAR(100),
        Phone NVARCHAR(50),
        Fax NVARCHAR(50),
        HomePage NVARCHAR(MAX),
        SOR_SK INT,
        staging_raw_id_nk INT,
        ValidFrom DATE
    );

    SELECT @SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_table_name}';

    IF @SOR_SK IS NULL
        THROW 50006, 'Dim_SOR does not contain the source table name for DimSuppliers.', 1;

    MERGE [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    USING (
        SELECT
            staging_raw_id_sk,
            SupplierID,
            CompanyName,
            ContactName,
            ContactTitle,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            Phone,
            Fax,
            HomePage
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE SupplierID IS NOT NULL
    ) AS SRC
        ON SRC.SupplierID = DST.SupplierID_NK
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            SupplierID_NK,
            CompanyName,
            ContactName,
            ContactTitle,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            Phone,
            Fax,
            HomePage,
            SOR_SK,
            staging_raw_id_nk,
            ValidFrom
        )
        VALUES (
            SRC.SupplierID,
            SRC.CompanyName,
            SRC.ContactName,
            SRC.ContactTitle,
            SRC.Address,
            SRC.City,
            SRC.Region,
            SRC.PostalCode,
            SRC.Country,
            SRC.Phone,
            SRC.Fax,
            SRC.HomePage,
            @SOR_SK,
            SRC.staging_raw_id_sk,
            @Today
        )
    WHEN MATCHED AND (
           ISNULL(DST.CompanyName, N'') <> ISNULL(SRC.CompanyName, N'')
        OR ISNULL(DST.ContactName, N'') <> ISNULL(SRC.ContactName, N'')
        OR ISNULL(DST.ContactTitle, N'') <> ISNULL(SRC.ContactTitle, N'')
        OR ISNULL(DST.Address, N'') <> ISNULL(SRC.Address, N'')
        OR ISNULL(DST.City, N'') <> ISNULL(SRC.City, N'')
        OR ISNULL(DST.Region, N'') <> ISNULL(SRC.Region, N'')
        OR ISNULL(DST.PostalCode, N'') <> ISNULL(SRC.PostalCode, N'')
        OR ISNULL(DST.Country, N'') <> ISNULL(SRC.Country, N'')
        OR ISNULL(DST.Phone, N'') <> ISNULL(SRC.Phone, N'')
        OR ISNULL(DST.Fax, N'') <> ISNULL(SRC.Fax, N'')
        OR ISNULL(DST.HomePage, N'') <> ISNULL(SRC.HomePage, N'')
    )
    THEN
        UPDATE SET
            DST.CompanyName = SRC.CompanyName,
            DST.ContactName = SRC.ContactName,
            DST.ContactTitle = SRC.ContactTitle,
            DST.Address = SRC.Address,
            DST.City = SRC.City,
            DST.Region = SRC.Region,
            DST.PostalCode = SRC.PostalCode,
            DST.Country = SRC.Country,
            DST.Phone = SRC.Phone,
            DST.Fax = SRC.Fax,
            DST.HomePage = SRC.HomePage,
            DST.SOR_SK = @SOR_SK,
            DST.staging_raw_id_nk = SRC.staging_raw_id_sk,
            DST.ValidFrom = @Today
    OUTPUT
        DELETED.SupplierID_NK,
        DELETED.CompanyName,
        DELETED.ContactName,
        DELETED.ContactTitle,
        DELETED.Address,
        DELETED.City,
        DELETED.Region,
        DELETED.PostalCode,
        DELETED.Country,
        DELETED.Phone,
        DELETED.Fax,
        DELETED.HomePage,
        DELETED.SOR_SK,
        DELETED.staging_raw_id_nk,
        DELETED.ValidFrom
    INTO @ChangedSuppliers;

    INSERT INTO [{database_name}].[{schema_name}].[DimSuppliers_History] (
        SupplierID_NK,
        CompanyName,
        ContactName,
        ContactTitle,
        Address,
        City,
        Region,
        PostalCode,
        Country,
        Phone,
        Fax,
        HomePage,
        SOR_SK,
        staging_raw_id_nk,
        ValidFrom,
        ValidTo
    )
    SELECT
        SupplierID_NK,
        CompanyName,
        ContactName,
        ContactTitle,
        Address,
        City,
        Region,
        PostalCode,
        Country,
        Phone,
        Fax,
        HomePage,
        SOR_SK,
        staging_raw_id_nk,
        ValidFrom,
        @Yesterday
    FROM @ChangedSuppliers
    WHERE SupplierID_NK IS NOT NULL;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
