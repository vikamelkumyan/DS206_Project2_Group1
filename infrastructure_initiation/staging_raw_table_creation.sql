/*
    DS206 Group Project #2
    File: infrastructure_initiation/staging_raw_table_creation.sql
    Purpose: Create staging raw/source tables for the ORDER_DDS database using the source table names from Table 1.

    Important design decision:
    - staging_raw_id_sk is the technical surrogate key of every staging table.
    - Original source keys from Table 1, such as CustomerID, ProductID, OrderID,
      are kept as normal source/natural key columns.
    - No source business key is used as the primary key of a staging table.
*/

USE ORDER_DDS;
GO

/*
    Drop staging raw/source tables if they already exist.
    These source/staging tables do not have foreign key constraints here, so the drop order is not critical.
*/
DROP TABLE IF EXISTS [dbo].[OrderDetails];
DROP TABLE IF EXISTS [dbo].[Orders];
DROP TABLE IF EXISTS [dbo].[Products];
DROP TABLE IF EXISTS [dbo].[Territories];
DROP TABLE IF EXISTS [dbo].[Suppliers];
DROP TABLE IF EXISTS [dbo].[Shippers];
DROP TABLE IF EXISTS [dbo].[Region];
DROP TABLE IF EXISTS [dbo].[Employees];
DROP TABLE IF EXISTS [dbo].[Customers];
DROP TABLE IF EXISTS [dbo].[Categories];
GO

/* =====================================================================================
   1. Categories
   Source natural key from Table 1: CategoryID
===================================================================================== */
CREATE TABLE [dbo].[Categories] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [CategoryID] INT NULL,
    [CategoryName] NVARCHAR(100) NULL,
    [Description] NVARCHAR(500) NULL,
    CONSTRAINT [PK_Categories] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   2. Customers
   Source natural key from Table 1: CustomerID
===================================================================================== */
CREATE TABLE [dbo].[Customers] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [CustomerID] NVARCHAR(10) NULL,
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
    CONSTRAINT [PK_Customers] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   3. Employees
   Source natural key from Table 1: EmployeeID
   ReportsTo is a source foreign key to Employees.EmployeeID from Table 2.
===================================================================================== */
CREATE TABLE [dbo].[Employees] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [EmployeeID] INT NULL,
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
    [ReportsTo] INT NULL,
    [PhotoPath] NVARCHAR(300) NULL,
    CONSTRAINT [PK_Employees] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   4. OrderDetails
   Source natural key from Table 1: OrderID + ProductID
   The project Table 1 writes this table as OrderDetails. The Excel sheet name may still be OrderDetails.
===================================================================================== */
CREATE TABLE [dbo].[OrderDetails] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [OrderID] INT NULL,
    [ProductID] INT NULL,
    [UnitPrice] DECIMAL(19,4) NULL,
    [Quantity] INT NULL,
    [Discount] DECIMAL(10,4) NULL,
    CONSTRAINT [PK_OrderDetails] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   5. Orders
   Source natural key from Table 1: OrderID
   CustomerID, EmployeeID, ShipVia, and TerritoryID are source foreign keys from Table 2.
===================================================================================== */
CREATE TABLE [dbo].[Orders] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [OrderID] INT NULL,
    [CustomerID] NVARCHAR(10) NULL,
    [EmployeeID] INT NULL,
    [OrderDate] DATE NULL,
    [RequiredDate] DATE NULL,
    [ShippedDate] DATE NULL,
    [ShipVia] INT NULL,
    [Freight] DECIMAL(19,4) NULL,
    [ShipName] NVARCHAR(100) NULL,
    [ShipAddress] NVARCHAR(200) NULL,
    [ShipCity] NVARCHAR(100) NULL,
    [ShipRegion] NVARCHAR(100) NULL,
    [ShipPostalCode] NVARCHAR(30) NULL,
    [ShipCountry] NVARCHAR(100) NULL,
    [TerritoryID] NVARCHAR(20) NULL,
    CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   6. Products
   Source natural key from Table 1: ProductID
   CategoryID and SupplierID are source foreign keys from Table 2.
===================================================================================== */
CREATE TABLE [dbo].[Products] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [ProductID] INT NULL,
    [ProductName] NVARCHAR(100) NULL,
    [SupplierID] INT NULL,
    [CategoryID] INT NULL,
    [QuantityPerUnit] NVARCHAR(100) NULL,
    [UnitPrice] DECIMAL(19,4) NULL,
    [UnitsInStock] INT NULL,
    [UnitsOnOrder] INT NULL,
    [ReorderLevel] INT NULL,
    [Discontinued] BIT NULL,
    CONSTRAINT [PK_Products] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   7. Region
   Source natural key from Table 1: RegionID
===================================================================================== */
CREATE TABLE [dbo].[Region] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [RegionID] INT NULL,
    [RegionDescription] NVARCHAR(100) NULL,
    [RegionCategory] NVARCHAR(50) NULL,
    [RegionImportance] NVARCHAR(50) NULL,
    CONSTRAINT [PK_Region] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   8. Shippers
   Source natural key from Table 1: ShipperID
===================================================================================== */
CREATE TABLE [dbo].[Shippers] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [ShipperID] INT NULL,
    [CompanyName] NVARCHAR(100) NULL,
    [Phone] NVARCHAR(50) NULL,
    CONSTRAINT [PK_Shippers] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   9. Suppliers
   Source natural key from Table 1: SupplierID
===================================================================================== */
CREATE TABLE [dbo].[Suppliers] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [SupplierID] INT NULL,
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
    CONSTRAINT [PK_Suppliers] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO

/* =====================================================================================
   10. Territories
   Source natural key from Table 1: TerritoryID
   RegionID is a source foreign key to Region.RegionID from Table 2.
===================================================================================== */
CREATE TABLE [dbo].[Territories] (
    [staging_raw_id_sk] INT IDENTITY(1,1) NOT NULL,
    [TerritoryID] NVARCHAR(20) NULL,
    [TerritoryDescription] NVARCHAR(100) NULL,
    [TerritoryCode] NVARCHAR(20) NULL,
    [RegionID] INT NULL,
    CONSTRAINT [PK_Territories] PRIMARY KEY CLUSTERED ([staging_raw_id_sk] ASC)
);
GO
