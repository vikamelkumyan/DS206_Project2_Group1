import pymssql

# This connects to the 'master' system which always exists
try:
    conn = pymssql.connect(
        server='127.0.0.1',
        user='sa',
        password='Password123!',
        database='master',
        autocommit=True 
    )
    cursor = conn.cursor()
    
    # Create the database your project needs
    cursor.execute("CREATE DATABASE ORDER_DDS")
    print("✅ SUCCESS: ORDER_DDS database created!")
    
    conn.close()
except Exception as e:
    print(f"❌ ERROR: {e}")