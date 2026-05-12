/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_employees.sql
    Purpose: Populate DimEmployees from staging_raw_Employees.

    SCD logic: SCD1 with delete.
    Parameters expected from Python .format():
        database_name
        schema_name
        source_table_name
        target_table_name
*/

SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @SOR_SK INT;

    SELECT @SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_table_name}';

    IF @SOR_SK IS NULL
        THROW 50004, 'Dim_SOR does not contain the source table name for DimEmployees.', 1;

    MERGE [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    USING (
        SELECT
            staging_raw_id_sk,
            EmployeeID,
            LastName,
            FirstName,
            Title,
            TitleOfCourtesy,
            BirthDate,
            HireDate,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            HomePhone,
            Extension,
            Notes,
            ReportsTo,
            PhotoPath
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE EmployeeID IS NOT NULL
    ) AS SRC
        ON SRC.EmployeeID = DST.EmployeeID_NK
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            EmployeeID_NK,
            LastName,
            FirstName,
            Title,
            TitleOfCourtesy,
            BirthDate,
            HireDate,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            HomePhone,
            Extension,
            Notes,
            ReportsToEmployeeID_NK,
            PhotoPath,
            SOR_SK,
            staging_raw_id_nk,
            IsDeleted,
            DeletedDate
        )
        VALUES (
            SRC.EmployeeID,
            SRC.LastName,
            SRC.FirstName,
            SRC.Title,
            SRC.TitleOfCourtesy,
            SRC.BirthDate,
            SRC.HireDate,
            SRC.Address,
            SRC.City,
            SRC.Region,
            SRC.PostalCode,
            SRC.Country,
            SRC.HomePhone,
            SRC.Extension,
            SRC.Notes,
            SRC.ReportsTo,
            SRC.PhotoPath,
            @SOR_SK,
            SRC.staging_raw_id_sk,
            0,
            NULL
        )
    WHEN MATCHED AND (
           ISNULL(DST.LastName, N'') <> ISNULL(SRC.LastName, N'')
        OR ISNULL(DST.FirstName, N'') <> ISNULL(SRC.FirstName, N'')
        OR ISNULL(DST.Title, N'') <> ISNULL(SRC.Title, N'')
        OR ISNULL(DST.TitleOfCourtesy, N'') <> ISNULL(SRC.TitleOfCourtesy, N'')
        OR ISNULL(DST.BirthDate, '19000101') <> ISNULL(SRC.BirthDate, '19000101')
        OR ISNULL(DST.HireDate, '19000101') <> ISNULL(SRC.HireDate, '19000101')
        OR ISNULL(DST.Address, N'') <> ISNULL(SRC.Address, N'')
        OR ISNULL(DST.City, N'') <> ISNULL(SRC.City, N'')
        OR ISNULL(DST.Region, N'') <> ISNULL(SRC.Region, N'')
        OR ISNULL(DST.PostalCode, N'') <> ISNULL(SRC.PostalCode, N'')
        OR ISNULL(DST.Country, N'') <> ISNULL(SRC.Country, N'')
        OR ISNULL(DST.HomePhone, N'') <> ISNULL(SRC.HomePhone, N'')
        OR ISNULL(DST.Extension, N'') <> ISNULL(SRC.Extension, N'')
        OR ISNULL(DST.Notes, N'') <> ISNULL(SRC.Notes, N'')
        OR ISNULL(DST.ReportsToEmployeeID_NK, -1) <> ISNULL(SRC.ReportsTo, -1)
        OR ISNULL(DST.PhotoPath, N'') <> ISNULL(SRC.PhotoPath, N'')
        OR ISNULL(DST.staging_raw_id_nk, -1) <> ISNULL(SRC.staging_raw_id_sk, -1)
        OR DST.SOR_SK <> @SOR_SK
        OR DST.IsDeleted = 1
    )
    THEN
        UPDATE SET
            DST.LastName = SRC.LastName,
            DST.FirstName = SRC.FirstName,
            DST.Title = SRC.Title,
            DST.TitleOfCourtesy = SRC.TitleOfCourtesy,
            DST.BirthDate = SRC.BirthDate,
            DST.HireDate = SRC.HireDate,
            DST.Address = SRC.Address,
            DST.City = SRC.City,
            DST.Region = SRC.Region,
            DST.PostalCode = SRC.PostalCode,
            DST.Country = SRC.Country,
            DST.HomePhone = SRC.HomePhone,
            DST.Extension = SRC.Extension,
            DST.Notes = SRC.Notes,
            DST.ReportsToEmployeeID_NK = SRC.ReportsTo,
            DST.PhotoPath = SRC.PhotoPath,
            DST.SOR_SK = @SOR_SK,
            DST.staging_raw_id_nk = SRC.staging_raw_id_sk,
            DST.IsDeleted = 0,
            DST.DeletedDate = NULL
    WHEN NOT MATCHED BY SOURCE THEN
        UPDATE SET
            DST.IsDeleted = 1,
            DST.DeletedDate = COALESCE(DST.DeletedDate, CAST(GETDATE() AS DATE));

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
