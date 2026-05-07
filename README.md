# DS206 Project 2 - Infrastructure Initialization

This repository contains infrastructure-as-code for setting up a SQL Server database with a dimensional data structure (DDS).

## Overview

This project initializes a SQL Server instance with:
- **Database**: `ORDER_DDS` (Order Dimensional Data Structure)
- **Configuration**: ODBC-based connection settings for secure database access
- **Platform**: Docker-based SQL Server 2022 instance

## Prerequisites

### Required Software
- **Docker**: SQL Server 2022 container
- **Python 3.7+**: For testing and validation (optional)
- **macOS/Linux**: This guide is optimized for Unix-like systems

### Setup Instructions

1. **Start SQL Server in Docker**:
   ```bash
   docker run -e "ACCEPT_EULA=Y" \
     -e "MSSQL_SA_PASSWORD=YourStrongPassword123" \
     -e "MSSQL_PID=developer" \
     -p 1433:1433 \
     --name sqlserver \
     -d mcr.microsoft.com/mssql/server:2022-latest
   ```

2. **Configure environment variables**:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and add your SQL Server password (this file is gitignored and never committed)

## Files in This Project

```
.
├── .env.example                         # Environment template (commit this)
├── .gitignore                           # Git ignore rules
├── README.md                            # This file
└── infrastructure_initiation/
    ├── dimensional_database_creation.sql    # SQL script to create ORDER_DDS database
    └── sql_server_config.cfg                # ODBC connection configuration
```

### dimensional_database_creation.sql
- Creates the `ORDER_DDS` database if it doesn't already exist
- Uses `IF DB_ID` check for idempotency (safe to run multiple times)
- T-SQL syntax compatible with SQL Server 2019+

### sql_server_config.cfg
- ODBC connection configuration file (no secrets)
- Specifies connection parameters (server, port, database, encryption)
- Uses encrypted connections with certificate validation disabled for development

## Quick Start

### 1. Verify SQL Server is Running

```bash
docker ps | grep sqlserver
```

Expected: Container should show `Up` status on port `0.0.0.0:1433->1433/tcp`

### 2. Set Up Environment

```bash
cp .env.example .env
# Edit .env with your SQL Server password
source .env
```

### 3. Execute the Setup Script

```bash
cat infrastructure_initiation/dimensional_database_creation.sql | \
  docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "$MSSQL_SA_PASSWORD" \
  -C
```

### 4. Verify Database Creation

```bash
echo "SELECT name FROM sys.databases WHERE name='ORDER_DDS';" | \
  docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "$MSSQL_SA_PASSWORD" \
  -C
```

Expected output:
```
name
ORDER_DDS

(1 rows affected)
```

## Configuration Reference

### sql_server_config.cfg Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `driver` | ODBC Driver 18 for SQL Server | Modern ODBC driver for SQL Server |
| `server` | localhost | SQL Server host |
| `port` | 1433 | Default SQL Server port |
| `database` | ORDER_DDS | Target database |
| `trusted_connection` | yes | Windows authentication (if applicable) |
| `encrypt` | yes | Enable connection encryption |
| `trust_server_certificate` | yes | Accept self-signed certificates (dev only) |

**Security Note**: For production environments, set `trust_server_certificate=no` and configure proper SSL certificates.

## Testing

A complete test suite was run to verify:
- ✓ SQL Server connectivity on localhost:1433
- ✓ SQL script syntax validation
- ✓ Database creation
- ✓ Idempotency (script runs multiple times safely)
- ✓ Configuration file format

**Result**: All tests passed

## Troubleshooting

### Connection Refused
- Ensure Docker container is running: `docker ps | grep sqlserver`
- Check port 1433 is accessible: `nc -zv localhost 1433`

### Authentication Failed
- Verify password in `.env` matches Docker `MSSQL_SA_PASSWORD`
- Confirm username is `sa` (system administrator)
- Ensure you've run `source .env`

### Certificate Errors
- Use `-C` flag with sqlcmd to trust self-signed certificate (development only)
- For production, configure proper SSL certificates

### Database Already Exists
- Script is idempotent and skips creation if database exists
- To reset: `DROP DATABASE ORDER_DDS;` (destructive operation)

## Reproducibility

This setup is fully reproducible on any machine with Docker:

1. **Idempotent Script**: `dimensional_database_creation.sql` safely handles re-execution
2. **Non-sensitive Configuration**: All tracked files (`sql_server_config.cfg`) contain no secrets
3. **Environment-based Secrets**: Passwords stored in `.env` (gitignored)
4. **Containerized Database**: Docker ensures consistent environment
5. **Version Locked**: SQL Server 2022-latest specified

To reproduce on a new machine:
1. Install Docker
2. Run SQL Server container with your own password
3. Copy `.env.example` to `.env` and fill in values
4. Execute the SQL script (follow "Quick Start" above)
5. Verify database creation

## Next Steps

After infrastructure initialization:
- Load dimensional data into `ORDER_DDS` database
- Create tables for dimensions and facts
- Set up indexes and constraints
- Configure backups and maintenance

## Contact

**Project**: DS206 - Project 2  
**Group**: Group 1  
**Last Updated**: 2026-05-07
