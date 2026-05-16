/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_territories.sql
    Purpose: Populate DimTerritories from Territories.

    SCD logic: SCD3.
    The SCD3-tracked attribute is TerritoryCode.
    Current value is kept in TerritoryCode.
    Previous value is kept in TerritoryCode_Prev1.
    Parameters expected from Python .format():
        database_name
        schema_name
        source_table_name
        target_table_name
*/

SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    DECLARE @SOR_SK INT;

    SELECT @SOR_SK = SOR_SK
    FROM [{database_name}].[{schema_name}].[Dim_SOR]
    WHERE staging_raw_table_name = N'{source_table_name}';

    IF @SOR_SK IS NULL
        THROW 50007, 'Dim_SOR does not contain the source table name for DimTerritories.', 1;

    MERGE [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    USING (
        SELECT
            terr.staging_raw_id_sk,
            terr.TerritoryID,
            terr.TerritoryDescription,
            terr.TerritoryCode,
            terr.RegionID,
            reg.RegionID_SK AS RegionID_SK_FK
        FROM [{database_name}].[{schema_name}].[{source_table_name}] AS terr
        LEFT JOIN [{database_name}].[{schema_name}].[DimRegion] AS reg
            ON terr.RegionID = reg.RegionID_NK
        WHERE terr.TerritoryID IS NOT NULL
    ) AS SRC
        ON SRC.TerritoryID = DST.TerritoryID_NK
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            TerritoryID_NK,
            TerritoryDescription,
            TerritoryCode,
            TerritoryCode_Prev1,
            TerritoryCode_Prev1_ValidTo,
            RegionID_NK,
            RegionID_SK_FK,
            SOR_SK,
            staging_raw_id_nk
        )
        VALUES (
            SRC.TerritoryID,
            SRC.TerritoryDescription,
            SRC.TerritoryCode,
            NULL,
            NULL,
            SRC.RegionID,
            SRC.RegionID_SK_FK,
            @SOR_SK,
            SRC.staging_raw_id_sk
        )
    WHEN MATCHED AND (
           ISNULL(DST.TerritoryDescription, N'') <> ISNULL(SRC.TerritoryDescription, N'')
        OR ISNULL(DST.TerritoryCode, N'') <> ISNULL(SRC.TerritoryCode, N'')
        OR ISNULL(DST.RegionID_NK, -1) <> ISNULL(SRC.RegionID, -1)
        OR ISNULL(DST.RegionID_SK_FK, -1) <> ISNULL(SRC.RegionID_SK_FK, -1)
        OR ISNULL(DST.staging_raw_id_nk, -1) <> ISNULL(SRC.staging_raw_id_sk, -1)
        OR DST.SOR_SK <> @SOR_SK
    )
    THEN
        UPDATE SET
            DST.TerritoryDescription = SRC.TerritoryDescription,
            DST.TerritoryCode_Prev1 =
                CASE
                    WHEN ISNULL(DST.TerritoryCode, N'') <> ISNULL(SRC.TerritoryCode, N'')
                    THEN DST.TerritoryCode
                    ELSE DST.TerritoryCode_Prev1
                END,
            DST.TerritoryCode_Prev1_ValidTo =
                CASE
                    WHEN ISNULL(DST.TerritoryCode, N'') <> ISNULL(SRC.TerritoryCode, N'')
                    THEN @Yesterday
                    ELSE DST.TerritoryCode_Prev1_ValidTo
                END,
            DST.TerritoryCode = SRC.TerritoryCode,
            DST.RegionID_NK = SRC.RegionID,
            DST.RegionID_SK_FK = SRC.RegionID_SK_FK,
            DST.SOR_SK = @SOR_SK,
            DST.staging_raw_id_nk = SRC.staging_raw_id_sk;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
