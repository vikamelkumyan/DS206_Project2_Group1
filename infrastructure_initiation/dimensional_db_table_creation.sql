/*
    DS206 Group Project #2 - GROUP 1
    File: infrastructure_initiation/dimensional_db_table_creation.sql
    Purpose: Create the dimensional database tables for ORDER_DDS.

    GROUP 1 requirements from Table 3:
    - DimCategories  : SCD1 with delete
    - DimCustomers   : SCD2
    - DimEmployees   : SCD1 with delete
    - DimProducts    : SCD2 with delete closing
    - DimRegion      : SCD1
    - DimShippers    : SCD1
    - DimSuppliers   : SCD4
    - DimTerritories : SCD3, one attribute, current and prior
    - FactOrders     : SNAPSHOT

    Main design rule:
    - *_SK columns are dimensional surrogate keys.
    - *_NK columns are natural keys copied from the source/staging tables.
    - staging_raw_id_nk stores the staging_raw_id_sk value from the respective staging table.
    - SOR_SK points to Dim_SOR and tells which staging table the record came from.
*/

USE ORDER_DDS;
GO

/* =====================================================================================
   DROP OBJECTS
   Drop fact tables first, then dependent dimensions, then base dimensions.
===================================================================================== */
DROP TABLE IF EXISTS [dbo].[FactOrders_Error];
DROP TABLE IF EXISTS [dbo].[FactOrders];
GO

DROP TABLE IF EXISTS [dbo].[DimProducts];
DROP TABLE IF EXISTS [dbo].[DimTerritories];
DROP TABLE IF EXISTS [dbo].[DimSuppliers_History];
DROP TABLE IF EXISTS [dbo].[DimSuppliers];
DROP TABLE IF EXISTS [dbo].[DimShippers];
DROP TABLE IF EXISTS [dbo].[DimRegion];
DROP TABLE IF EXISTS [dbo].[DimEmployees];
DROP TABLE IF EXISTS [dbo].[DimCustomers];
DROP TABLE IF EXISTS [dbo].[DimCategories];
DROP TABLE IF EXISTS [dbo].[Dim_SOR];
GO

DROP SEQUENCE IF EXISTS [dbo].[CustomerID_DURABLE_SK_Seq];
DROP SEQUENCE IF EXISTS [dbo].[ProductID_DURABLE_SK_Seq];
GO

/* =====================================================================================
   STEP 6: Dim_SOR
   System of Record dimension. The project requires the key and the source/staging table name.
===================================================================================== */
CREATE TABLE [dbo].[Dim_SOR] (
    [SOR_SK] INT IDENTITY(1,1) NOT NULL,
    [staging_raw_table_name] NVARCHAR(128) NOT NULL,
    CONSTRAINT [PK_Dim_SOR] PRIMARY KEY CLUSTERED ([SOR_SK] ASC),
    CONSTRAINT [UQ_Dim_SOR_staging_raw_table_name] UNIQUE ([staging_raw_table_name])
);
GO

INSERT INTO [dbo].[Dim_SOR] ([staging_raw_table_name])
VALUES
    (N'Categories'),
    (N'Customers'),
    (N'Employees'),
    (N'OrderDetails'),
    (N'Orders'),
    (N'Products'),
    (N'Region'),
    (N'Shippers'),
    (N'Suppliers'),
    (N'Territories');
GO

/* =====================================================================================
   Sequences for SCD2 durable surrogate keys.
   The table surrogate key changes per version, while the durable key remains stable.
===================================================================================== */
CREATE SEQUENCE [dbo].[CustomerID_DURABLE_SK_Seq]
    AS INT
    START WITH 1
    INCREMENT BY 1;
GO

CREATE SEQUENCE [dbo].[ProductID_DURABLE_SK_Seq]
    AS INT
    START WITH 1
    INCREMENT BY 1;
GO

/* =====================================================================================
   DimCategories - SCD1 with delete
   Source: Categories
   Natural key: CategoryID
===================================================================================== */
CREATE TABLE [dbo].[DimCategories] (
    [CategoryID_SK] INT IDENTITY(1,1) NOT NULL,
    [CategoryID_NK] INT NOT NULL,
    [CategoryName] NVARCHAR(100) NULL,
    [Description] NVARCHAR(500) NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,

    CONSTRAINT [PK_DimCategories] PRIMARY KEY CLUSTERED ([CategoryID_SK] ASC),
    CONSTRAINT [UQ_DimCategories_CategoryID_NK] UNIQUE ([CategoryID_NK]),
    CONSTRAINT [FK_DimCategories_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

/* =====================================================================================
   DimCustomers - SCD2
   Source: Customers
   Natural key: CustomerID
===================================================================================== */
CREATE TABLE [dbo].[DimCustomers] (
    [CustomerID_TABLE_SK] INT IDENTITY(1,1) NOT NULL,
    [CustomerID_DURABLE_SK] INT NOT NULL,
    [CustomerID_NK] NVARCHAR(10) NOT NULL,
    [CompanyName] NVARCHAR(100) NULL,
    [ContactName] NVARCHAR(100) NULL,
    [ContactTitle] NVARCHAR(100) NULL,
    [Address] NVARCHAR(200) NULL,
    [City] NVARCHAR(100) NULL,
    [Region] NVARCHAR(100) NULL,
    [PostalCode] NVARCHAR(30) NULL,
    [Country] NVARCHAR(100) NULL,
    [Phone] NVARCHAR(50) NULL,
    [Fax] NVARCHAR(50) NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,

    [ValidFrom] DATE NOT NULL,
    [ValidTo] DATE NULL,
    [IsCurrent] BIT NOT NULL CONSTRAINT [DF_DimCustomers_IsCurrent] DEFAULT (1),

    CONSTRAINT [PK_DimCustomers] PRIMARY KEY CLUSTERED ([CustomerID_TABLE_SK] ASC),
    CONSTRAINT [FK_DimCustomers_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

CREATE UNIQUE INDEX [UX_DimCustomers_Current]
    ON [dbo].[DimCustomers] ([CustomerID_NK])
    WHERE [IsCurrent] = 1;
GO

/* =====================================================================================
   DimEmployees - SCD1 with delete
   Source: Employees
   Natural key: EmployeeID
===================================================================================== */
CREATE TABLE [dbo].[DimEmployees] (
    [EmployeeID_SK] INT IDENTITY(1,1) NOT NULL,
    [EmployeeID_NK] INT NOT NULL,
    [LastName] NVARCHAR(50) NULL,
    [FirstName] NVARCHAR(50) NULL,
    [Title] NVARCHAR(100) NULL,
    [TitleOfCourtesy] NVARCHAR(50) NULL,
    [BirthDate] DATE NULL,
    [HireDate] DATE NULL,
    [Address] NVARCHAR(200) NULL,
    [City] NVARCHAR(100) NULL,
    [Region] NVARCHAR(100) NULL,
    [PostalCode] NVARCHAR(30) NULL,
    [Country] NVARCHAR(100) NULL,
    [HomePhone] NVARCHAR(50) NULL,
    [Extension] NVARCHAR(20) NULL,
    [Notes] NVARCHAR(MAX) NULL,
    [ReportsToEmployeeID_NK] INT NULL,
    [PhotoPath] NVARCHAR(300) NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,

    CONSTRAINT [PK_DimEmployees] PRIMARY KEY CLUSTERED ([EmployeeID_SK] ASC),
    CONSTRAINT [UQ_DimEmployees_EmployeeID_NK] UNIQUE ([EmployeeID_NK]),
    CONSTRAINT [FK_DimEmployees_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

/* =====================================================================================
   DimRegion - SCD1
   Source: Region
   Natural key: RegionID
===================================================================================== */
CREATE TABLE [dbo].[DimRegion] (
    [RegionID_SK] INT IDENTITY(1,1) NOT NULL,
    [RegionID_NK] INT NOT NULL,
    [RegionDescription] NVARCHAR(100) NULL,
    [RegionCategory] NVARCHAR(50) NULL,
    [RegionImportance] NVARCHAR(50) NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,

    CONSTRAINT [PK_DimRegion] PRIMARY KEY CLUSTERED ([RegionID_SK] ASC),
    CONSTRAINT [UQ_DimRegion_RegionID_NK] UNIQUE ([RegionID_NK]),
    CONSTRAINT [FK_DimRegion_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

/* =====================================================================================
   DimShippers - SCD1
   Source: Shippers
   Natural key: ShipperID
===================================================================================== */
CREATE TABLE [dbo].[DimShippers] (
    [ShipperID_SK] INT IDENTITY(1,1) NOT NULL,
    [ShipperID_NK] INT NOT NULL,
    [CompanyName] NVARCHAR(100) NULL,
    [Phone] NVARCHAR(50) NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,

    CONSTRAINT [PK_DimShippers] PRIMARY KEY CLUSTERED ([ShipperID_SK] ASC),
    CONSTRAINT [UQ_DimShippers_ShipperID_NK] UNIQUE ([ShipperID_NK]),
    CONSTRAINT [FK_DimShippers_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

/* =====================================================================================
   DimSuppliers - SCD4
   Source: Suppliers
   Natural key: SupplierID
   SCD4 structure: current version in DimSuppliers, previous versions in DimSuppliers_History.
===================================================================================== */
CREATE TABLE [dbo].[DimSuppliers] (
    [SupplierID_SK] INT IDENTITY(1,1) NOT NULL,
    [SupplierID_NK] INT NOT NULL,
    [CompanyName] NVARCHAR(100) NULL,
    [ContactName] NVARCHAR(100) NULL,
    [ContactTitle] NVARCHAR(100) NULL,
    [Address] NVARCHAR(200) NULL,
    [City] NVARCHAR(100) NULL,
    [Region] NVARCHAR(100) NULL,
    [PostalCode] NVARCHAR(30) NULL,
    [Country] NVARCHAR(100) NULL,
    [Phone] NVARCHAR(50) NULL,
    [Fax] NVARCHAR(50) NULL,
    [HomePage] NVARCHAR(MAX) NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,
    [ValidFrom] DATE NOT NULL,

    CONSTRAINT [PK_DimSuppliers] PRIMARY KEY CLUSTERED ([SupplierID_SK] ASC),
    CONSTRAINT [UQ_DimSuppliers_SupplierID_NK] UNIQUE ([SupplierID_NK]),
    CONSTRAINT [FK_DimSuppliers_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

CREATE TABLE [dbo].[DimSuppliers_History] (
    [SupplierHistoryID_SK] INT IDENTITY(1,1) NOT NULL,
    [SupplierID_NK] INT NOT NULL,
    [CompanyName] NVARCHAR(100) NULL,
    [ContactName] NVARCHAR(100) NULL,
    [ContactTitle] NVARCHAR(100) NULL,
    [Address] NVARCHAR(200) NULL,
    [City] NVARCHAR(100) NULL,
    [Region] NVARCHAR(100) NULL,
    [PostalCode] NVARCHAR(30) NULL,
    [Country] NVARCHAR(100) NULL,
    [Phone] NVARCHAR(50) NULL,
    [Fax] NVARCHAR(50) NULL,
    [HomePage] NVARCHAR(MAX) NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,
    [ValidFrom] DATE NULL,
    [ValidTo] DATE NULL,

    CONSTRAINT [PK_DimSuppliers_History] PRIMARY KEY CLUSTERED ([SupplierHistoryID_SK] ASC),
    CONSTRAINT [FK_DimSuppliers_History_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

/* =====================================================================================
   DimTerritories - SCD3
   Source: Territories
   Natural key: TerritoryID
   One selected SCD3-tracked attribute: TerritoryCode.
   Current value is TerritoryCode, prior value is TerritoryCode_Prev1.
===================================================================================== */
CREATE TABLE [dbo].[DimTerritories] (
    [TerritoryID_SK] INT IDENTITY(1,1) NOT NULL,
    [TerritoryID_NK] NVARCHAR(20) NOT NULL,
    [TerritoryDescription] NVARCHAR(100) NULL,
    [TerritoryCode] NVARCHAR(20) NULL,
    [TerritoryCode_Prev1] NVARCHAR(20) NULL,
    [TerritoryCode_Prev1_ValidTo] DATE NULL,
    [RegionID_NK] INT NULL,
    [RegionID_SK_FK] INT NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,

    CONSTRAINT [PK_DimTerritories] PRIMARY KEY CLUSTERED ([TerritoryID_SK] ASC),
    CONSTRAINT [UQ_DimTerritories_TerritoryID_NK] UNIQUE ([TerritoryID_NK]),
    CONSTRAINT [FK_DimTerritories_DimRegion] FOREIGN KEY ([RegionID_SK_FK]) REFERENCES [dbo].[DimRegion] ([RegionID_SK]),
    CONSTRAINT [FK_DimTerritories_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

/* =====================================================================================
   DimProducts - SCD2 with delete closing
   Source: Products
   Natural key: ProductID
   CategoryID and SupplierID are source natural keys. CategoryID_SK_FK and SupplierID_SK_FK
   point to the current dimensional records used during the load.
===================================================================================== */
CREATE TABLE [dbo].[DimProducts] (
    [ProductID_TABLE_SK] INT IDENTITY(1,1) NOT NULL,
    [ProductID_DURABLE_SK] INT NOT NULL,
    [ProductID_NK] INT NOT NULL,
    [ProductName] NVARCHAR(100) NULL,
    [SupplierID_NK] INT NULL,
    [SupplierID_SK_FK] INT NULL,
    [CategoryID_NK] INT NULL,
    [CategoryID_SK_FK] INT NULL,
    [QuantityPerUnit] NVARCHAR(100) NULL,
    [UnitPrice] DECIMAL(19,4) NULL,
    [UnitsInStock] INT NULL,
    [UnitsOnOrder] INT NULL,
    [ReorderLevel] INT NULL,
    [Discontinued] BIT NULL,

    [SOR_SK] INT NOT NULL,
    [staging_raw_id_nk] INT NOT NULL,

    [ValidFrom] DATE NOT NULL,
    [ValidTo] DATE NULL,
    [IsCurrent] BIT NOT NULL CONSTRAINT [DF_DimProducts_IsCurrent] DEFAULT (1),
    CONSTRAINT [PK_DimProducts] PRIMARY KEY CLUSTERED ([ProductID_TABLE_SK] ASC),
    CONSTRAINT [FK_DimProducts_DimCategories] FOREIGN KEY ([CategoryID_SK_FK]) REFERENCES [dbo].[DimCategories] ([CategoryID_SK]) ON DELETE SET NULL,
    CONSTRAINT [FK_DimProducts_DimSuppliers] FOREIGN KEY ([SupplierID_SK_FK]) REFERENCES [dbo].[DimSuppliers] ([SupplierID_SK]),
    CONSTRAINT [FK_DimProducts_Dim_SOR] FOREIGN KEY ([SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

CREATE UNIQUE INDEX [UX_DimProducts_Current]
    ON [dbo].[DimProducts] ([ProductID_NK])
    WHERE [IsCurrent] = 1;
GO

/* =====================================================================================
   FactOrders - SNAPSHOT
   Grain: one row per order-product line, based on OrderID + ProductID.
   Source: Orders joined with OrderDetails.
===================================================================================== */
CREATE TABLE [dbo].[FactOrders] (
    [FactOrder_SK] INT IDENTITY(1,1) NOT NULL,

    [OrderID_NK] INT NOT NULL,
    [ProductID_NK] INT NOT NULL,

    [CustomerID_TABLE_SK_FK] INT NOT NULL,
    [EmployeeID_SK_FK] INT NOT NULL,
    [ProductID_TABLE_SK_FK] INT NOT NULL,
    [ShipperID_SK_FK] INT NOT NULL,
    [TerritoryID_SK_FK] INT NOT NULL,

    [OrderDate] DATE NULL,
    [RequiredDate] DATE NULL,
    [ShippedDate] DATE NULL,

    [Freight] DECIMAL(19,4) NULL,
    [UnitPrice] DECIMAL(19,4) NULL,
    [Quantity] INT NULL,
    [Discount] DECIMAL(10,4) NULL,
    [SalesAmount] DECIMAL(19,4) NULL,

    [ShipName] NVARCHAR(100) NULL,
    [ShipAddress] NVARCHAR(200) NULL,
    [ShipCity] NVARCHAR(100) NULL,
    [ShipRegion] NVARCHAR(100) NULL,
    [ShipPostalCode] NVARCHAR(30) NULL,
    [ShipCountry] NVARCHAR(100) NULL,

    [Orders_SOR_SK] INT NOT NULL,
    [Orders_staging_raw_id_nk] INT NOT NULL,
    [OrderDetails_SOR_SK] INT NOT NULL,
    [OrderDetails_staging_raw_id_nk] INT NOT NULL,

    [SnapshotDate] DATE NOT NULL CONSTRAINT [DF_FactOrders_SnapshotDate] DEFAULT (CAST(GETDATE() AS DATE)),
    [LoadDate] DATETIME2(0) NOT NULL CONSTRAINT [DF_FactOrders_LoadDate] DEFAULT (SYSDATETIME()),

    CONSTRAINT [PK_FactOrders] PRIMARY KEY CLUSTERED ([FactOrder_SK] ASC),
    CONSTRAINT [UQ_FactOrders_Order_Product] UNIQUE ([OrderID_NK], [ProductID_NK]),
    CONSTRAINT [FK_FactOrders_DimCustomers] FOREIGN KEY ([CustomerID_TABLE_SK_FK]) REFERENCES [dbo].[DimCustomers] ([CustomerID_TABLE_SK]),
    CONSTRAINT [FK_FactOrders_DimEmployees] FOREIGN KEY ([EmployeeID_SK_FK]) REFERENCES [dbo].[DimEmployees] ([EmployeeID_SK]),
    CONSTRAINT [FK_FactOrders_DimProducts] FOREIGN KEY ([ProductID_TABLE_SK_FK]) REFERENCES [dbo].[DimProducts] ([ProductID_TABLE_SK]),
    CONSTRAINT [FK_FactOrders_DimShippers] FOREIGN KEY ([ShipperID_SK_FK]) REFERENCES [dbo].[DimShippers] ([ShipperID_SK]),
    CONSTRAINT [FK_FactOrders_DimTerritories] FOREIGN KEY ([TerritoryID_SK_FK]) REFERENCES [dbo].[DimTerritories] ([TerritoryID_SK]),
    CONSTRAINT [FK_FactOrders_Orders_Dim_SOR] FOREIGN KEY ([Orders_SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK]),
    CONSTRAINT [FK_FactOrders_OrderDetails_Dim_SOR] FOREIGN KEY ([OrderDetails_SOR_SK]) REFERENCES [dbo].[Dim_SOR] ([SOR_SK])
);
GO

/* =====================================================================================
   FactOrders_Error
   Stores rows that cannot be loaded into FactOrders because of missing/invalid natural keys.
===================================================================================== */
CREATE TABLE [dbo].[FactOrders_Error] (
    [FactOrder_Error_SK] INT IDENTITY(1,1) NOT NULL,

    [OrderID_NK] INT NULL,
    [ProductID_NK] INT NULL,
    [CustomerID_NK] NVARCHAR(10) NULL,
    [EmployeeID_NK] INT NULL,
    [ShipperID_NK] INT NULL,
    [TerritoryID_NK] NVARCHAR(20) NULL,

    [OrderDate] DATE NULL,
    [RequiredDate] DATE NULL,
    [ShippedDate] DATE NULL,
    [Freight] DECIMAL(19,4) NULL,
    [UnitPrice] DECIMAL(19,4) NULL,
    [Quantity] INT NULL,
    [Discount] DECIMAL(10,4) NULL,

    [Orders_SOR_SK] INT NULL,
    [Orders_staging_raw_id_nk] INT NULL,
    [OrderDetails_SOR_SK] INT NULL,
    [OrderDetails_staging_raw_id_nk] INT NULL,

    [ErrorReason] NVARCHAR(500) NULL,
    [RejectedDate] DATETIME2(0) NOT NULL CONSTRAINT [DF_FactOrders_Error_RejectedDate] DEFAULT (SYSDATETIME()),

    CONSTRAINT [PK_FactOrders_Error] PRIMARY KEY CLUSTERED ([FactOrder_Error_SK] ASC)
);
GO
