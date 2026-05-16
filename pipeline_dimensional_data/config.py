from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INFRASTRUCTURE_DIR = PROJECT_ROOT / "infrastructure_initiation"
QUERIES_DIR = PROJECT_ROOT / "pipeline_dimensional_data" / "queries"
RAW_DATA_SOURCE_PATH = PROJECT_ROOT / "raw_data_source.xlsx"
LOG_FILE_PATH = PROJECT_ROOT / "logs" / "logs_dimensional_data_pipeline.txt"
SQL_SERVER_CONFIG_PATH = INFRASTRUCTURE_DIR / "sql_server_config.cfg"
ENV_PATH = PROJECT_ROOT / ".env"

DATABASE_NAME = "ORDER_DDS"
SCHEMA_NAME = "dbo"

SOURCE_SHEETS = [
    "Categories",
    "Customers",
    "Employees",
    "OrderDetails",
    "Orders",
    "Products",
    "Region",
    "Shippers",
    "Suppliers",
    "Territories",
]

SOURCE_TABLES = {
    sheet_name: sheet_name
    for sheet_name in SOURCE_SHEETS
}

DIMENSION_LOAD_ORDER = [
    {
        "name": "categories",
        "query": "update_dim_categories.sql",
        "source_table_name": SOURCE_TABLES["Categories"],
        "target_table_name": "DimCategories",
    },
    {
        "name": "region",
        "query": "update_dim_region.sql",
        "source_table_name": SOURCE_TABLES["Region"],
        "target_table_name": "DimRegion",
    },
    {
        "name": "shippers",
        "query": "update_dim_shippers.sql",
        "source_table_name": SOURCE_TABLES["Shippers"],
        "target_table_name": "DimShippers",
    },
    {
        "name": "suppliers",
        "query": "update_dim_suppliers.sql",
        "source_table_name": SOURCE_TABLES["Suppliers"],
        "target_table_name": "DimSuppliers",
    },
    {
        "name": "employees",
        "query": "update_dim_employees.sql",
        "source_table_name": SOURCE_TABLES["Employees"],
        "target_table_name": "DimEmployees",
    },
    {
        "name": "customers",
        "query": "update_dim_customers.sql",
        "source_table_name": SOURCE_TABLES["Customers"],
        "target_table_name": "DimCustomers",
    },
    {
        "name": "territories",
        "query": "update_dim_territories.sql",
        "source_table_name": SOURCE_TABLES["Territories"],
        "target_table_name": "DimTerritories",
    },
    {
        "name": "products",
        "query": "update_dim_products.sql",
        "source_table_name": SOURCE_TABLES["Products"],
        "target_table_name": "DimProducts",
    },
]

FACT_CONFIG = {
    "query": "update_fact.sql",
    "source_orders_table_name": SOURCE_TABLES["Orders"],
    "source_order_details_table_name": SOURCE_TABLES["OrderDetails"],
    "target_table_name": "FactOrders",
}

FACT_ERROR_CONFIG = {
    "query": "update_fact_error.sql",
    "source_orders_table_name": SOURCE_TABLES["Orders"],
    "source_order_details_table_name": SOURCE_TABLES["OrderDetails"],
    "target_table_name": "FactOrders_Error",
}
