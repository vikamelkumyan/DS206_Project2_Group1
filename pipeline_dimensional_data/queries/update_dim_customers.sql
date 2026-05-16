/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_customers.sql
    Purpose: Populate DimCustomers from Customers.

    SCD logic: SCD2.
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

    SELECT @SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_table_name}';

    IF @SOR_SK IS NULL
        THROW 50005, 'Dim_SOR does not contain the source table name for DimCustomers.', 1;

    ;WITH SourceRows AS (
        SELECT
            staging_raw_id_sk,
            CustomerID,
            CompanyName,
            ContactName,
            ContactTitle,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            Phone,
            Fax
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE CustomerID IS NOT NULL
    )
    UPDATE DST
    SET
        DST.ValidTo = @Yesterday,
        DST.IsCurrent = 0
    FROM [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    INNER JOIN SourceRows AS SRC
        ON SRC.CustomerID = DST.CustomerID_NK
    WHERE DST.IsCurrent = 1
      AND (
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
      );

    ;WITH SourceRows AS (
        SELECT
            staging_raw_id_sk,
            CustomerID,
            CompanyName,
            ContactName,
            ContactTitle,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            Phone,
            Fax
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE CustomerID IS NOT NULL
    )
    INSERT INTO [{database_name}].[{schema_name}].[{target_table_name}] (
        CustomerID_DURABLE_SK,
        CustomerID_NK,
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
        SOR_SK,
        staging_raw_id_nk,
        ValidFrom,
        ValidTo,
        IsCurrent
    )
    SELECT
        NEXT VALUE FOR [{database_name}].[{schema_name}].[CustomerID_DURABLE_SK_Seq],
        SRC.CustomerID,
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
        @SOR_SK,
        SRC.staging_raw_id_sk,
        @Today,
        NULL,
        1
    FROM SourceRows AS SRC
    WHERE NOT EXISTS (
        SELECT 1
        FROM [{database_name}].[{schema_name}].[{target_table_name}] AS DST
        WHERE DST.CustomerID_NK = SRC.CustomerID
    );

    ;WITH SourceRows AS (
        SELECT
            staging_raw_id_sk,
            CustomerID,
            CompanyName,
            ContactName,
            ContactTitle,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            Phone,
            Fax
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE CustomerID IS NOT NULL
    )
    INSERT INTO [{database_name}].[{schema_name}].[{target_table_name}] (
        CustomerID_DURABLE_SK,
        CustomerID_NK,
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
        SOR_SK,
        staging_raw_id_nk,
        ValidFrom,
        ValidTo,
        IsCurrent
    )
    SELECT
        PreviousVersion.CustomerID_DURABLE_SK,
        SRC.CustomerID,
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
        @SOR_SK,
        SRC.staging_raw_id_sk,
        @Today,
        NULL,
        1
    FROM SourceRows AS SRC
    CROSS APPLY (
        SELECT TOP 1 *
        FROM [{database_name}].[{schema_name}].[{target_table_name}] AS HIST
        WHERE HIST.CustomerID_NK = SRC.CustomerID
        ORDER BY HIST.CustomerID_TABLE_SK DESC
    ) AS PreviousVersion
    WHERE NOT EXISTS (
        SELECT 1
        FROM [{database_name}].[{schema_name}].[{target_table_name}] AS CurrentVersion
        WHERE CurrentVersion.CustomerID_NK = SRC.CustomerID
          AND CurrentVersion.IsCurrent = 1
    )
      AND (
           ISNULL(PreviousVersion.CompanyName, N'') <> ISNULL(SRC.CompanyName, N'')
        OR ISNULL(PreviousVersion.ContactName, N'') <> ISNULL(SRC.ContactName, N'')
        OR ISNULL(PreviousVersion.ContactTitle, N'') <> ISNULL(SRC.ContactTitle, N'')
        OR ISNULL(PreviousVersion.Address, N'') <> ISNULL(SRC.Address, N'')
        OR ISNULL(PreviousVersion.City, N'') <> ISNULL(SRC.City, N'')
        OR ISNULL(PreviousVersion.Region, N'') <> ISNULL(SRC.Region, N'')
        OR ISNULL(PreviousVersion.PostalCode, N'') <> ISNULL(SRC.PostalCode, N'')
        OR ISNULL(PreviousVersion.Country, N'') <> ISNULL(SRC.Country, N'')
        OR ISNULL(PreviousVersion.Phone, N'') <> ISNULL(SRC.Phone, N'')
        OR ISNULL(PreviousVersion.Fax, N'') <> ISNULL(SRC.Fax, N'')
      );

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
