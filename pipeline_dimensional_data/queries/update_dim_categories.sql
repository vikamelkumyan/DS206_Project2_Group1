/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_categories.sql
    Purpose: Populate DimCategories from staging_raw_Categories.

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
        THROW 50001, 'Dim_SOR does not contain the source table name for DimCategories.', 1;

    MERGE [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    USING (
        SELECT
            staging_raw_id_sk,
            CategoryID,
            CategoryName,
            Description
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE CategoryID IS NOT NULL
    ) AS SRC
        ON SRC.CategoryID = DST.CategoryID_NK
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            CategoryID_NK,
            CategoryName,
            Description,
            SOR_SK,
            staging_raw_id_nk,
            IsDeleted,
            DeletedDate
        )
        VALUES (
            SRC.CategoryID,
            SRC.CategoryName,
            SRC.Description,
            @SOR_SK,
            SRC.staging_raw_id_sk,
            0,
            NULL
        )
    WHEN MATCHED AND (
           ISNULL(DST.CategoryName, N'') <> ISNULL(SRC.CategoryName, N'')
        OR ISNULL(DST.Description, N'') <> ISNULL(SRC.Description, N'')
        OR ISNULL(DST.staging_raw_id_nk, -1) <> ISNULL(SRC.staging_raw_id_sk, -1)
        OR DST.SOR_SK <> @SOR_SK
        OR DST.IsDeleted = 1
    )
    THEN
        UPDATE SET
            DST.CategoryName = SRC.CategoryName,
            DST.Description = SRC.Description,
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
