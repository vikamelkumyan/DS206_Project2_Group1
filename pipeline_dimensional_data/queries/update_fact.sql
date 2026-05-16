/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_fact.sql
    Purpose: Populate FactOrders from Orders, OrderDetails, and dimension tables.

    Fact logic: SNAPSHOT / MERGE-based load.
    Grain: one row per order-product line, identified by OrderID_NK + ProductID_NK.

    Parameters expected from Python .format():
        database_name
        schema_name
        source_orders_table_name
        source_order_details_table_name
        target_table_name
        start_date
        end_date

    Important:
    - This script loads only valid fact rows.
    - Rows with missing/invalid natural keys are intentionally excluded here.
    - Those rejected rows should be handled in update_fact_error.sql in Step 9.
*/

SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @StartDate DATE = CONVERT(DATE, '{start_date}');
    DECLARE @EndDate   DATE = CONVERT(DATE, '{end_date}');
    DECLARE @Orders_SOR_SK INT;
    DECLARE @OrderDetails_SOR_SK INT;

    IF @StartDate IS NULL OR @EndDate IS NULL
        THROW 50101, 'start_date and end_date must be valid dates.', 1;

    IF @StartDate > @EndDate
        THROW 50102, 'start_date cannot be greater than end_date.', 1;

    SELECT @Orders_SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_orders_table_name}';

    SELECT @OrderDetails_SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_order_details_table_name}';

    IF @Orders_SOR_SK IS NULL
        THROW 50103, 'Dim_SOR does not contain the source table name for Orders.', 1;

    IF @OrderDetails_SOR_SK IS NULL
        THROW 50104, 'Dim_SOR does not contain the source table name for OrderDetails.', 1;

    ;WITH FactSource AS (
        SELECT
            o.OrderID AS OrderID_NK,
            od.ProductID AS ProductID_NK,

            dc.CustomerID_TABLE_SK AS CustomerID_TABLE_SK_FK,
            de.EmployeeID_SK AS EmployeeID_SK_FK,
            dp.ProductID_TABLE_SK AS ProductID_TABLE_SK_FK,
            ds.ShipperID_SK AS ShipperID_SK_FK,
            dt.TerritoryID_SK AS TerritoryID_SK_FK,

            o.OrderDate,
            o.RequiredDate,
            o.ShippedDate,

            o.Freight,
            od.UnitPrice,
            od.Quantity,
            od.Discount,
            CAST(
                ISNULL(od.UnitPrice, 0) * ISNULL(od.Quantity, 0) * (1 - ISNULL(od.Discount, 0))
                AS DECIMAL(19,4)
            ) AS SalesAmount,

            o.ShipName,
            o.ShipAddress,
            o.ShipCity,
            o.ShipRegion,
            o.ShipPostalCode,
            o.ShipCountry,

            @Orders_SOR_SK AS Orders_SOR_SK,
            o.staging_raw_id_sk AS Orders_staging_raw_id_nk,
            @OrderDetails_SOR_SK AS OrderDetails_SOR_SK,
            od.staging_raw_id_sk AS OrderDetails_staging_raw_id_nk,

            ROW_NUMBER() OVER (
                PARTITION BY o.OrderID, od.ProductID
                ORDER BY o.staging_raw_id_sk DESC, od.staging_raw_id_sk DESC
            ) AS rn
        FROM [{database_name}].[{schema_name}].[{source_orders_table_name}] AS o
        INNER JOIN [{database_name}].[{schema_name}].[{source_order_details_table_name}] AS od
            ON o.OrderID = od.OrderID
        LEFT JOIN [{database_name}].[{schema_name}].[DimCustomers] AS dc
            ON o.CustomerID = dc.CustomerID_NK
           AND dc.IsCurrent = 1
        LEFT JOIN [{database_name}].[{schema_name}].[DimEmployees] AS de
            ON o.EmployeeID = de.EmployeeID_NK
        LEFT JOIN [{database_name}].[{schema_name}].[DimProducts] AS dp
            ON od.ProductID = dp.ProductID_NK
           AND dp.IsCurrent = 1
        LEFT JOIN [{database_name}].[{schema_name}].[DimShippers] AS ds
            ON o.ShipVia = ds.ShipperID_NK
        LEFT JOIN [{database_name}].[{schema_name}].[DimTerritories] AS dt
            ON o.TerritoryID = dt.TerritoryID_NK
        WHERE o.OrderDate >= @StartDate
          AND o.OrderDate <= @EndDate
    ),
    ValidFactSource AS (
        SELECT
            OrderID_NK,
            ProductID_NK,
            CustomerID_TABLE_SK_FK,
            EmployeeID_SK_FK,
            ProductID_TABLE_SK_FK,
            ShipperID_SK_FK,
            TerritoryID_SK_FK,
            OrderDate,
            RequiredDate,
            ShippedDate,
            Freight,
            UnitPrice,
            Quantity,
            Discount,
            SalesAmount,
            ShipName,
            ShipAddress,
            ShipCity,
            ShipRegion,
            ShipPostalCode,
            ShipCountry,
            Orders_SOR_SK,
            Orders_staging_raw_id_nk,
            OrderDetails_SOR_SK,
            OrderDetails_staging_raw_id_nk
        FROM FactSource
        WHERE rn = 1
          AND OrderID_NK IS NOT NULL
          AND ProductID_NK IS NOT NULL
          AND CustomerID_TABLE_SK_FK IS NOT NULL
          AND EmployeeID_SK_FK IS NOT NULL
          AND ProductID_TABLE_SK_FK IS NOT NULL
          AND ShipperID_SK_FK IS NOT NULL
          AND TerritoryID_SK_FK IS NOT NULL
    )
    MERGE [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    USING ValidFactSource AS SRC
        ON  DST.OrderID_NK = SRC.OrderID_NK
        AND DST.ProductID_NK = SRC.ProductID_NK
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            OrderID_NK,
            ProductID_NK,
            CustomerID_TABLE_SK_FK,
            EmployeeID_SK_FK,
            ProductID_TABLE_SK_FK,
            ShipperID_SK_FK,
            TerritoryID_SK_FK,
            OrderDate,
            RequiredDate,
            ShippedDate,
            Freight,
            UnitPrice,
            Quantity,
            Discount,
            SalesAmount,
            ShipName,
            ShipAddress,
            ShipCity,
            ShipRegion,
            ShipPostalCode,
            ShipCountry,
            Orders_SOR_SK,
            Orders_staging_raw_id_nk,
            OrderDetails_SOR_SK,
            OrderDetails_staging_raw_id_nk,
            SnapshotDate,
            LoadDate
        )
        VALUES (
            SRC.OrderID_NK,
            SRC.ProductID_NK,
            SRC.CustomerID_TABLE_SK_FK,
            SRC.EmployeeID_SK_FK,
            SRC.ProductID_TABLE_SK_FK,
            SRC.ShipperID_SK_FK,
            SRC.TerritoryID_SK_FK,
            SRC.OrderDate,
            SRC.RequiredDate,
            SRC.ShippedDate,
            SRC.Freight,
            SRC.UnitPrice,
            SRC.Quantity,
            SRC.Discount,
            SRC.SalesAmount,
            SRC.ShipName,
            SRC.ShipAddress,
            SRC.ShipCity,
            SRC.ShipRegion,
            SRC.ShipPostalCode,
            SRC.ShipCountry,
            SRC.Orders_SOR_SK,
            SRC.Orders_staging_raw_id_nk,
            SRC.OrderDetails_SOR_SK,
            SRC.OrderDetails_staging_raw_id_nk,
            CAST(GETDATE() AS DATE),
            SYSDATETIME()
        )
    WHEN MATCHED AND (
           ISNULL(DST.CustomerID_TABLE_SK_FK, -1) <> ISNULL(SRC.CustomerID_TABLE_SK_FK, -1)
        OR ISNULL(DST.EmployeeID_SK_FK, -1) <> ISNULL(SRC.EmployeeID_SK_FK, -1)
        OR ISNULL(DST.ProductID_TABLE_SK_FK, -1) <> ISNULL(SRC.ProductID_TABLE_SK_FK, -1)
        OR ISNULL(DST.ShipperID_SK_FK, -1) <> ISNULL(SRC.ShipperID_SK_FK, -1)
        OR ISNULL(DST.TerritoryID_SK_FK, -1) <> ISNULL(SRC.TerritoryID_SK_FK, -1)
        OR ISNULL(DST.OrderDate, '19000101') <> ISNULL(SRC.OrderDate, '19000101')
        OR ISNULL(DST.RequiredDate, '19000101') <> ISNULL(SRC.RequiredDate, '19000101')
        OR ISNULL(DST.ShippedDate, '19000101') <> ISNULL(SRC.ShippedDate, '19000101')
        OR ISNULL(DST.Freight, -1) <> ISNULL(SRC.Freight, -1)
        OR ISNULL(DST.UnitPrice, -1) <> ISNULL(SRC.UnitPrice, -1)
        OR ISNULL(DST.Quantity, -1) <> ISNULL(SRC.Quantity, -1)
        OR ISNULL(DST.Discount, -1) <> ISNULL(SRC.Discount, -1)
        OR ISNULL(DST.SalesAmount, -1) <> ISNULL(SRC.SalesAmount, -1)
        OR ISNULL(DST.ShipName, N'') <> ISNULL(SRC.ShipName, N'')
        OR ISNULL(DST.ShipAddress, N'') <> ISNULL(SRC.ShipAddress, N'')
        OR ISNULL(DST.ShipCity, N'') <> ISNULL(SRC.ShipCity, N'')
        OR ISNULL(DST.ShipRegion, N'') <> ISNULL(SRC.ShipRegion, N'')
        OR ISNULL(DST.ShipPostalCode, N'') <> ISNULL(SRC.ShipPostalCode, N'')
        OR ISNULL(DST.ShipCountry, N'') <> ISNULL(SRC.ShipCountry, N'')
        OR ISNULL(DST.Orders_SOR_SK, -1) <> ISNULL(SRC.Orders_SOR_SK, -1)
        OR ISNULL(DST.Orders_staging_raw_id_nk, -1) <> ISNULL(SRC.Orders_staging_raw_id_nk, -1)
        OR ISNULL(DST.OrderDetails_SOR_SK, -1) <> ISNULL(SRC.OrderDetails_SOR_SK, -1)
        OR ISNULL(DST.OrderDetails_staging_raw_id_nk, -1) <> ISNULL(SRC.OrderDetails_staging_raw_id_nk, -1)
    )
    THEN
        UPDATE SET
            DST.CustomerID_TABLE_SK_FK = SRC.CustomerID_TABLE_SK_FK,
            DST.EmployeeID_SK_FK = SRC.EmployeeID_SK_FK,
            DST.ProductID_TABLE_SK_FK = SRC.ProductID_TABLE_SK_FK,
            DST.ShipperID_SK_FK = SRC.ShipperID_SK_FK,
            DST.TerritoryID_SK_FK = SRC.TerritoryID_SK_FK,
            DST.OrderDate = SRC.OrderDate,
            DST.RequiredDate = SRC.RequiredDate,
            DST.ShippedDate = SRC.ShippedDate,
            DST.Freight = SRC.Freight,
            DST.UnitPrice = SRC.UnitPrice,
            DST.Quantity = SRC.Quantity,
            DST.Discount = SRC.Discount,
            DST.SalesAmount = SRC.SalesAmount,
            DST.ShipName = SRC.ShipName,
            DST.ShipAddress = SRC.ShipAddress,
            DST.ShipCity = SRC.ShipCity,
            DST.ShipRegion = SRC.ShipRegion,
            DST.ShipPostalCode = SRC.ShipPostalCode,
            DST.ShipCountry = SRC.ShipCountry,
            DST.Orders_SOR_SK = SRC.Orders_SOR_SK,
            DST.Orders_staging_raw_id_nk = SRC.Orders_staging_raw_id_nk,
            DST.OrderDetails_SOR_SK = SRC.OrderDetails_SOR_SK,
            DST.OrderDetails_staging_raw_id_nk = SRC.OrderDetails_staging_raw_id_nk,
            DST.SnapshotDate = CAST(GETDATE() AS DATE),
            DST.LoadDate = SYSDATETIME()
    WHEN NOT MATCHED BY SOURCE
         AND DST.OrderDate >= @StartDate
         AND DST.OrderDate <= @EndDate
    THEN DELETE;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
