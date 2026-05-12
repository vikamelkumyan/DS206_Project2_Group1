import pymssql

def create_tables():
    try:
        # Connect to your new database
        conn = pymssql.connect(
            server='127.0.0.1',
            user='sa',
            password='Password123!',
            database='ORDER_DDS'
        )
        cursor = conn.cursor()

        # Simple SQL to create the Customers staging table
        # You can add the other Group 1 tables here too!
        sql = """
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE name = 'stg_Customers')
        CREATE TABLE stg_Customers (
            CustomerID NVARCHAR(50),
            CompanyName NVARCHAR(255),
            ContactName NVARCHAR(255),
            ContactTitle NVARCHAR(255),
            Address NVARCHAR(255),
            City NVARCHAR(255),
            Region NVARCHAR(255),
            PostalCode NVARCHAR(255),
            Country NVARCHAR(255),
            Phone NVARCHAR(255),
            Fax NVARCHAR(255)
        );
        """
        
        cursor.execute(sql)
        conn.commit()
        print("✅ SUCCESS: stg_Customers table created!")
        conn.close()
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    create_tables()