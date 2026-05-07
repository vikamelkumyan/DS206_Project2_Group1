USE ORDER_DDS;
GO

DROP TABLE IF EXISTS dbo.stg_Categories;
CREATE TABLE dbo.stg_Categories (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Customers;
CREATE TABLE dbo.stg_Customers (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(5) NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Employees;
CREATE TABLE dbo.stg_Employees (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_OrderDetails;
CREATE TABLE dbo.stg_OrderDetails (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Orders;
CREATE TABLE dbo.stg_Orders (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Products;
CREATE TABLE dbo.stg_Products (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Region;
CREATE TABLE dbo.stg_Region (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    RegionID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Shippers;
CREATE TABLE dbo.stg_Shippers (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Suppliers;
CREATE TABLE dbo.stg_Suppliers (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT NOT NULL
);
GO

DROP TABLE IF EXISTS dbo.stg_Territories;
CREATE TABLE dbo.stg_Territories (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID NVARCHAR(20) NOT NULL
);
GO
