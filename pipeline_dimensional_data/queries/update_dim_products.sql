/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_products.sql
    Purpose: Populate DimProducts from Products.

    SCD logic: SCD2 with delete closing.
    Delete handling: when a source product disappears, close the current row by setting IsCurrent = 0.
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
        THROW 50008, 'Dim_SOR does not contain the source table name for DimProducts.', 1;

    /*
        1. Close current SCD2 rows for products that disappeared from the source.
           The row is closed only by IsCurrent = 0.
    */
    ;WITH SourceRows AS (
        SELECT
            prod.staging_raw_id_sk,
            prod.ProductID,
            prod.ProductName,
            prod.SupplierID,
            sup.SupplierID_SK AS SupplierID_SK_FK,
            prod.CategoryID,
            cat.CategoryID_SK AS CategoryID_SK_FK,
            prod.QuantityPerUnit,
            prod.UnitPrice,
            prod.UnitsInStock,
            prod.UnitsOnOrder,
            prod.ReorderLevel,
            prod.Discontinued
        FROM [{database_name}].[{schema_name}].[{source_table_name}] AS prod
        LEFT JOIN [{database_name}].[{schema_name}].[DimSuppliers] AS sup
            ON prod.SupplierID = sup.SupplierID_NK
        LEFT JOIN [{database_name}].[{schema_name}].[DimCategories] AS cat
            ON prod.CategoryID = cat.CategoryID_NK
        WHERE prod.ProductID IS NOT NULL
    )
    UPDATE DST
    SET
        DST.ValidTo = @Yesterday,
        DST.IsCurrent = 0
    FROM [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    LEFT JOIN SourceRows AS SRC
        ON SRC.ProductID = DST.ProductID_NK
    WHERE DST.IsCurrent = 1
      AND SRC.ProductID IS NULL;

    /*
        2. Close current SCD2 rows for products whose tracked attributes changed.
    */
    ;WITH SourceRows AS (
        SELECT
            prod.staging_raw_id_sk,
            prod.ProductID,
            prod.ProductName,
            prod.SupplierID,
            sup.SupplierID_SK AS SupplierID_SK_FK,
            prod.CategoryID,
            cat.CategoryID_SK AS CategoryID_SK_FK,
            prod.QuantityPerUnit,
            prod.UnitPrice,
            prod.UnitsInStock,
            prod.UnitsOnOrder,
            prod.ReorderLevel,
            prod.Discontinued
        FROM [{database_name}].[{schema_name}].[{source_table_name}] AS prod
        LEFT JOIN [{database_name}].[{schema_name}].[DimSuppliers] AS sup
            ON prod.SupplierID = sup.SupplierID_NK
        LEFT JOIN [{database_name}].[{schema_name}].[DimCategories] AS cat
            ON prod.CategoryID = cat.CategoryID_NK
        WHERE prod.ProductID IS NOT NULL
    )
    UPDATE DST
    SET
        DST.ValidTo = @Yesterday,
        DST.IsCurrent = 0
    FROM [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    INNER JOIN SourceRows AS SRC
        ON SRC.ProductID = DST.ProductID_NK
    WHERE DST.IsCurrent = 1
      AND (
           ISNULL(DST.ProductName, N'') <> ISNULL(SRC.ProductName, N'')
        OR ISNULL(DST.SupplierID_NK, -1) <> ISNULL(SRC.SupplierID, -1)
        OR ISNULL(DST.SupplierID_SK_FK, -1) <> ISNULL(SRC.SupplierID_SK_FK, -1)
        OR ISNULL(DST.CategoryID_NK, -1) <> ISNULL(SRC.CategoryID, -1)
        OR ISNULL(DST.CategoryID_SK_FK, -1) <> ISNULL(SRC.CategoryID_SK_FK, -1)
        OR ISNULL(DST.QuantityPerUnit, N'') <> ISNULL(SRC.QuantityPerUnit, N'')
        OR ISNULL(DST.UnitPrice, -1) <> ISNULL(SRC.UnitPrice, -1)
        OR ISNULL(DST.UnitsInStock, -1) <> ISNULL(SRC.UnitsInStock, -1)
        OR ISNULL(DST.UnitsOnOrder, -1) <> ISNULL(SRC.UnitsOnOrder, -1)
        OR ISNULL(DST.ReorderLevel, -1) <> ISNULL(SRC.ReorderLevel, -1)
        OR ISNULL(CAST(DST.Discontinued AS INT), -1) <> ISNULL(CAST(SRC.Discontinued AS INT), -1)
        OR ISNULL(DST.staging_raw_id_nk, -1) <> ISNULL(SRC.staging_raw_id_sk, -1)
        OR DST.SOR_SK <> @SOR_SK
      );

    /*
        3. Insert products that never existed before.
           These get a new durable surrogate key.
    */
    ;WITH SourceRows AS (
        SELECT
            prod.staging_raw_id_sk,
            prod.ProductID,
            prod.ProductName,
            prod.SupplierID,
            sup.SupplierID_SK AS SupplierID_SK_FK,
            prod.CategoryID,
            cat.CategoryID_SK AS CategoryID_SK_FK,
            prod.QuantityPerUnit,
            prod.UnitPrice,
            prod.UnitsInStock,
            prod.UnitsOnOrder,
            prod.ReorderLevel,
            prod.Discontinued
        FROM [{database_name}].[{schema_name}].[{source_table_name}] AS prod
        LEFT JOIN [{database_name}].[{schema_name}].[DimSuppliers] AS sup
            ON prod.SupplierID = sup.SupplierID_NK
        LEFT JOIN [{database_name}].[{schema_name}].[DimCategories] AS cat
            ON prod.CategoryID = cat.CategoryID_NK
        WHERE prod.ProductID IS NOT NULL
    )
    INSERT INTO [{database_name}].[{schema_name}].[{target_table_name}] (
        ProductID_DURABLE_SK,
        ProductID_NK,
        ProductName,
        SupplierID_NK,
        SupplierID_SK_FK,
        CategoryID_NK,
        CategoryID_SK_FK,
        QuantityPerUnit,
        UnitPrice,
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        SOR_SK,
        staging_raw_id_nk,
        ValidFrom,
        ValidTo,
        IsCurrent
    )
    SELECT
        NEXT VALUE FOR [{database_name}].[{schema_name}].[ProductID_DURABLE_SK_Seq],
        SRC.ProductID,
        SRC.ProductName,
        SRC.SupplierID,
        SRC.SupplierID_SK_FK,
        SRC.CategoryID,
        SRC.CategoryID_SK_FK,
        SRC.QuantityPerUnit,
        SRC.UnitPrice,
        SRC.UnitsInStock,
        SRC.UnitsOnOrder,
        SRC.ReorderLevel,
        SRC.Discontinued,
        @SOR_SK,
        SRC.staging_raw_id_sk,
        @Today,
        NULL,
        1
    FROM SourceRows AS SRC
    WHERE NOT EXISTS (
        SELECT 1
        FROM [{database_name}].[{schema_name}].[{target_table_name}] AS DST
        WHERE DST.ProductID_NK = SRC.ProductID
    );

    /*
        4. Insert a new current version for source products that have history but no current row.
           This covers both normal changes and delete-then-reappear cases.
    */
    ;WITH SourceRows AS (
        SELECT
            prod.staging_raw_id_sk,
            prod.ProductID,
            prod.ProductName,
            prod.SupplierID,
            sup.SupplierID_SK AS SupplierID_SK_FK,
            prod.CategoryID,
            cat.CategoryID_SK AS CategoryID_SK_FK,
            prod.QuantityPerUnit,
            prod.UnitPrice,
            prod.UnitsInStock,
            prod.UnitsOnOrder,
            prod.ReorderLevel,
            prod.Discontinued
        FROM [{database_name}].[{schema_name}].[{source_table_name}] AS prod
        LEFT JOIN [{database_name}].[{schema_name}].[DimSuppliers] AS sup
            ON prod.SupplierID = sup.SupplierID_NK
        LEFT JOIN [{database_name}].[{schema_name}].[DimCategories] AS cat
            ON prod.CategoryID = cat.CategoryID_NK
        WHERE prod.ProductID IS NOT NULL
    )
    INSERT INTO [{database_name}].[{schema_name}].[{target_table_name}] (
        ProductID_DURABLE_SK,
        ProductID_NK,
        ProductName,
        SupplierID_NK,
        SupplierID_SK_FK,
        CategoryID_NK,
        CategoryID_SK_FK,
        QuantityPerUnit,
        UnitPrice,
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        SOR_SK,
        staging_raw_id_nk,
        ValidFrom,
        ValidTo,
        IsCurrent
    )
    SELECT
        PreviousVersion.ProductID_DURABLE_SK,
        SRC.ProductID,
        SRC.ProductName,
        SRC.SupplierID,
        SRC.SupplierID_SK_FK,
        SRC.CategoryID,
        SRC.CategoryID_SK_FK,
        SRC.QuantityPerUnit,
        SRC.UnitPrice,
        SRC.UnitsInStock,
        SRC.UnitsOnOrder,
        SRC.ReorderLevel,
        SRC.Discontinued,
        @SOR_SK,
        SRC.staging_raw_id_sk,
        @Today,
        NULL,
        1
    FROM SourceRows AS SRC
    CROSS APPLY (
        SELECT TOP 1 *
        FROM [{database_name}].[{schema_name}].[{target_table_name}] AS HIST
        WHERE HIST.ProductID_NK = SRC.ProductID
        ORDER BY HIST.ProductID_TABLE_SK DESC
    ) AS PreviousVersion
    WHERE NOT EXISTS (
        SELECT 1
        FROM [{database_name}].[{schema_name}].[{target_table_name}] AS CurrentVersion
        WHERE CurrentVersion.ProductID_NK = SRC.ProductID
          AND CurrentVersion.IsCurrent = 1
    );

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
