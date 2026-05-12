import pymssql

def create_all_staging_tables():
    conn = pymssql.connect(server='127.0.0.1', user='sa', password='Password123!', database='ORDER_DDS')
    cursor = conn.cursor()

    # The list of all staging tables needed for the project
    tables_sql = [
        "CREATE TABLE stg_Products (ProductID INT, ProductName NVARCHAR(255), CategoryID INT, UnitPrice DECIMAL(10,2))",
        "CREATE TABLE stg_Orders (OrderID INT, CustomerID NVARCHAR(50), EmployeeID INT, OrderDate DATETIME, ShipCity NVARCHAR(255))",
        "CREATE TABLE stg_Categories (CategoryID INT, CategoryName NVARCHAR(255), Description NVARCHAR(MAX))",
        "CREATE TABLE stg_Suppliers (SupplierID INT, CompanyName NVARCHAR(255), ContactName NVARCHAR(255))",
        "CREATE TABLE stg_Employees (EmployeeID INT, LastName NVARCHAR(255), FirstName NVARCHAR(255), Title NVARCHAR(255))"
    ]

    for sql in tables_sql:
        table_name = sql.split()[2]
        try:
            # Drop if exists to ensure a clean start for the full task
            cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
            cursor.execute(sql)
            print(f"✅ Created {table_name}")
        except Exception as e:
            print(f"❌ Failed to create {table_name}: {e}")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_all_staging_tables()