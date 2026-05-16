/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_fact_error.sql
    Purpose: Populate FactOrders_Error with rows rejected from FactOrders.

    Fact error logic:
    - This script captures rows that cannot be loaded into FactOrders because of
      missing or invalid natural keys.
    - The same main business grain as FactOrders is used: one row per order-product line.
    - Orders without matching order-detail rows are captured, because they cannot form
      the required order-product grain.
    - OrderDetails rows with no matching Orders row are also captured. Their OrderDate
      is stored as NULL because the parent order row is missing.

    Parameters expected from Python .format():
        database_name
        schema_name
        source_orders_table_name
        source_order_details_table_name
        target_table_name
        start_date
        end_date

    Important:
    - Do not put GO statements in this file, because it is intended to be executed
      from Python through pyodbc after parameter replacement.
    - The script first deletes old error rows for the selected date range and then
      inserts the currently rejected rows. This makes the load reproducible.
*/

SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @StartDate DATE = CONVERT(DATE, '{start_date}');
    DECLARE @EndDate   DATE = CONVERT(DATE, '{end_date}');
    DECLARE @Orders_SOR_SK INT;
    DECLARE @OrderDetails_SOR_SK INT;
    DECLARE @InsertedRows INT;

    IF @StartDate IS NULL OR @EndDate IS NULL
        THROW 50201, 'start_date and end_date must be valid dates.', 1;

    IF @StartDate > @EndDate
        THROW 50202, 'start_date cannot be greater than end_date.', 1;

    SELECT @Orders_SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_orders_table_name}';

    SELECT @OrderDetails_SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_order_details_table_name}';

    IF @Orders_SOR_SK IS NULL
        THROW 50203, 'Dim_SOR does not contain the source table name for Orders.', 1;

    IF @OrderDetails_SOR_SK IS NULL
        THROW 50204, 'Dim_SOR does not contain the source table name for OrderDetails.', 1;

    /*
        Remove previous rejected rows for the same source tables.
        For normal order-based rows, the date range is based on Orders.OrderDate.
        For orphan OrderDetails rows, OrderDate is NULL, so those are also refreshed.
    */
    DELETE FROM [{database_name}].[{schema_name}].[{target_table_name}]
    WHERE (
              OrderDate >= @StartDate
          AND OrderDate <= @EndDate
          OR  OrderDate IS NULL
          )
      AND Orders_SOR_SK = @Orders_SOR_SK
      AND OrderDetails_SOR_SK = @OrderDetails_SOR_SK;

    ;WITH OrderBasedFactErrorSource AS (
        SELECT
            o.OrderID AS OrderID_NK,
            od.ProductID AS ProductID_NK,
            o.CustomerID AS CustomerID_NK,
            o.EmployeeID AS EmployeeID_NK,
            o.ShipVia AS ShipperID_NK,
            o.TerritoryID AS TerritoryID_NK,

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

            @Orders_SOR_SK AS Orders_SOR_SK,
            o.staging_raw_id_sk AS Orders_staging_raw_id_nk,
            @OrderDetails_SOR_SK AS OrderDetails_SOR_SK,
            od.staging_raw_id_sk AS OrderDetails_staging_raw_id_nk,

            CONCAT(
                CASE
                    WHEN o.OrderID IS NULL THEN N'Missing OrderID; '
                    ELSE N''
                END,
                CASE
                    WHEN od.staging_raw_id_sk IS NULL THEN N'Missing OrderDetails row for OrderID; '
                    ELSE N''
                END,
                CASE
                    WHEN od.staging_raw_id_sk IS NOT NULL AND od.ProductID IS NULL THEN N'Missing ProductID; '
                    WHEN od.staging_raw_id_sk IS NOT NULL AND od.ProductID IS NOT NULL AND dp.ProductID_TABLE_SK IS NULL THEN N'Invalid ProductID_NK, product is missing or not current; '
                    ELSE N''
                END,
                CASE
                    WHEN o.CustomerID IS NULL THEN N'Missing CustomerID; '
                    WHEN o.CustomerID IS NOT NULL AND dc.CustomerID_TABLE_SK IS NULL THEN N'Invalid CustomerID_NK; '
                    ELSE N''
                END,
                CASE
                    WHEN o.EmployeeID IS NULL THEN N'Missing EmployeeID; '
                    WHEN o.EmployeeID IS NOT NULL AND de.EmployeeID_SK IS NULL THEN N'Invalid EmployeeID_NK, employee is missing; '
                    ELSE N''
                END,
                CASE
                    WHEN o.ShipVia IS NULL THEN N'Missing ShipVia/ShipperID; '
                    WHEN o.ShipVia IS NOT NULL AND ds.ShipperID_SK IS NULL THEN N'Invalid ShipVia/ShipperID_NK; '
                    ELSE N''
                END,
                CASE
                    WHEN o.TerritoryID IS NULL THEN N'Missing TerritoryID; '
                    WHEN o.TerritoryID IS NOT NULL AND dt.TerritoryID_SK IS NULL THEN N'Invalid TerritoryID_NK; '
                    ELSE N''
                END
            ) AS ErrorReason
        FROM [{database_name}].[{schema_name}].[{source_orders_table_name}] AS o
        LEFT JOIN [{database_name}].[{schema_name}].[{source_order_details_table_name}] AS od
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
    OrphanOrderDetailsErrorSource AS (
        SELECT
            od.OrderID AS OrderID_NK,
            od.ProductID AS ProductID_NK,
            CAST(NULL AS NVARCHAR(10)) AS CustomerID_NK,
            CAST(NULL AS INT) AS EmployeeID_NK,
            CAST(NULL AS INT) AS ShipperID_NK,
            CAST(NULL AS NVARCHAR(20)) AS TerritoryID_NK,

            CAST(NULL AS INT) AS CustomerID_TABLE_SK_FK,
            CAST(NULL AS INT) AS EmployeeID_SK_FK,
            dp.ProductID_TABLE_SK AS ProductID_TABLE_SK_FK,
            CAST(NULL AS INT) AS ShipperID_SK_FK,
            CAST(NULL AS INT) AS TerritoryID_SK_FK,

            CAST(NULL AS DATE) AS OrderDate,
            CAST(NULL AS DATE) AS RequiredDate,
            CAST(NULL AS DATE) AS ShippedDate,
            CAST(NULL AS DECIMAL(19,4)) AS Freight,
            od.UnitPrice,
            od.Quantity,
            od.Discount,

            @Orders_SOR_SK AS Orders_SOR_SK,
            CAST(NULL AS INT) AS Orders_staging_raw_id_nk,
            @OrderDetails_SOR_SK AS OrderDetails_SOR_SK,
            od.staging_raw_id_sk AS OrderDetails_staging_raw_id_nk,

            CONCAT(
                CASE
                    WHEN od.OrderID IS NULL THEN N'Missing OrderID in OrderDetails; '
                    ELSE N'Invalid OrderID_NK in OrderDetails, matching Orders row is missing; '
                END,
                CASE
                    WHEN od.ProductID IS NULL THEN N'Missing ProductID; '
                    WHEN od.ProductID IS NOT NULL AND dp.ProductID_TABLE_SK IS NULL THEN N'Invalid ProductID_NK, product is missing or not current; '
                    ELSE N''
                END
            ) AS ErrorReason
        FROM [{database_name}].[{schema_name}].[{source_order_details_table_name}] AS od
        LEFT JOIN [{database_name}].[{schema_name}].[{source_orders_table_name}] AS o
            ON od.OrderID = o.OrderID
        LEFT JOIN [{database_name}].[{schema_name}].[DimProducts] AS dp
            ON od.ProductID = dp.ProductID_NK
           AND dp.IsCurrent = 1
        WHERE o.staging_raw_id_sk IS NULL
    ),
    FactErrorSource AS (
        SELECT * FROM OrderBasedFactErrorSource
        UNION ALL
        SELECT * FROM OrphanOrderDetailsErrorSource
    ),
    RejectedFactRows AS (
        SELECT
            OrderID_NK,
            ProductID_NK,
            CustomerID_NK,
            EmployeeID_NK,
            ShipperID_NK,
            TerritoryID_NK,
            OrderDate,
            RequiredDate,
            ShippedDate,
            Freight,
            UnitPrice,
            Quantity,
            Discount,
            Orders_SOR_SK,
            Orders_staging_raw_id_nk,
            OrderDetails_SOR_SK,
            OrderDetails_staging_raw_id_nk,
            ErrorReason
        FROM FactErrorSource
        WHERE (
                 OrderID_NK IS NULL
              OR OrderDetails_staging_raw_id_nk IS NULL
              OR ProductID_NK IS NULL
              OR CustomerID_TABLE_SK_FK IS NULL
              OR EmployeeID_SK_FK IS NULL
              OR ProductID_TABLE_SK_FK IS NULL
              OR ShipperID_SK_FK IS NULL
              OR TerritoryID_SK_FK IS NULL
          )
    )
    INSERT INTO [{database_name}].[{schema_name}].[{target_table_name}] (
        OrderID_NK,
        ProductID_NK,
        CustomerID_NK,
        EmployeeID_NK,
        ShipperID_NK,
        TerritoryID_NK,
        OrderDate,
        RequiredDate,
        ShippedDate,
        Freight,
        UnitPrice,
        Quantity,
        Discount,
        Orders_SOR_SK,
        Orders_staging_raw_id_nk,
        OrderDetails_SOR_SK,
        OrderDetails_staging_raw_id_nk,
        ErrorReason,
        RejectedDate
    )
    SELECT
        OrderID_NK,
        ProductID_NK,
        CustomerID_NK,
        EmployeeID_NK,
        ShipperID_NK,
        TerritoryID_NK,
        OrderDate,
        RequiredDate,
        ShippedDate,
        Freight,
        UnitPrice,
        Quantity,
        Discount,
        Orders_SOR_SK,
        Orders_staging_raw_id_nk,
        OrderDetails_SOR_SK,
        OrderDetails_staging_raw_id_nk,
        NULLIF(ErrorReason, N''),
        SYSDATETIME()
    FROM RejectedFactRows;

    SET @InsertedRows = @@ROWCOUNT;

    COMMIT TRANSACTION;

    SELECT @InsertedRows AS inserted_fact_error_rows;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
