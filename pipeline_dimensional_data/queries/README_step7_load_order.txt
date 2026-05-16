DS206 Group Project #2 - GROUP 1
Step 7 SQL scripts for pipeline_dimensional_data/queries

Recommended dimension load order:
1. update_dim_categories.sql
2. update_dim_region.sql
3. update_dim_shippers.sql
4. update_dim_suppliers.sql
5. update_dim_territories.sql
6. update_dim_customers.sql
7. update_dim_employees.sql
8. update_dim_products.sql

Reason:
- DimTerritories needs DimRegion.
- DimProducts needs DimCategories and DimSuppliers.
- Customers, Employees, Shippers are independent for the fact load.

These scripts are parametrized for Python .format() replacement.
Common parameters:
- database_name
- schema_name
- source_table_name
- target_table_name

Do not run these raw in SSMS without replacing the placeholders first.


Source/staging table parameters should use the Table 1 source names, for example Categories, Customers, Employees, OrderDetails, Orders, Products, Region, Shippers, Suppliers, Territories.
