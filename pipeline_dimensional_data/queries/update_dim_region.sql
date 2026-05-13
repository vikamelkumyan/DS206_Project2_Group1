/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_region.sql
    Purpose: Populate DimRegion from staging_raw_Region.

    SCD logic: SCD1.
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
        THROW 50002, 'Dim_SOR does not contain the source table name for DimRegion.', 1;

    MERGE [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    USING (
        SELECT
            staging_raw_id_sk,
            RegionID,
            RegionDescription,
            RegionCategory,
            RegionImportance
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE RegionID IS NOT NULL
    ) AS SRC
        ON SRC.RegionID = DST.RegionID_NK
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            RegionID_NK,
            RegionDescription,
            RegionCategory,
            RegionImportance,
            SOR_SK,
            staging_raw_id_nk
        )
        VALUES (
            SRC.RegionID,
            SRC.RegionDescription,
            SRC.RegionCategory,
            SRC.RegionImportance,
            @SOR_SK,
            SRC.staging_raw_id_sk
        )
    WHEN MATCHED AND (
           ISNULL(DST.RegionDescription, N'') <> ISNULL(SRC.RegionDescription, N'')
        OR ISNULL(DST.RegionCategory, N'') <> ISNULL(SRC.RegionCategory, N'')
        OR ISNULL(DST.RegionImportance, N'') <> ISNULL(SRC.RegionImportance, N'')
        OR ISNULL(DST.staging_raw_id_nk, -1) <> ISNULL(SRC.staging_raw_id_sk, -1)
        OR DST.SOR_SK <> @SOR_SK
    )
    THEN
        UPDATE SET
            DST.RegionDescription = SRC.RegionDescription,
            DST.RegionCategory = SRC.RegionCategory,
            DST.RegionImportance = SRC.RegionImportance,
            DST.SOR_SK = @SOR_SK,
            DST.staging_raw_id_nk = SRC.staging_raw_id_sk;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
