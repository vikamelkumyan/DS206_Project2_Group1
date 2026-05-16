/*
    DS206 Group Project #2 - GROUP 1
    File: pipeline_dimensional_data/queries/update_dim_shippers.sql
    Purpose: Populate DimShippers from Shippers.

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
        THROW 50003, 'Dim_SOR does not contain the source table name for DimShippers.', 1;

    MERGE [{database_name}].[{schema_name}].[{target_table_name}] AS DST
    USING (
        SELECT
            staging_raw_id_sk,
            ShipperID,
            CompanyName,
            Phone
        FROM [{database_name}].[{schema_name}].[{source_table_name}]
        WHERE ShipperID IS NOT NULL
    ) AS SRC
        ON SRC.ShipperID = DST.ShipperID_NK
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            ShipperID_NK,
            CompanyName,
            Phone,
            SOR_SK,
            staging_raw_id_nk
        )
        VALUES (
            SRC.ShipperID,
            SRC.CompanyName,
            SRC.Phone,
            @SOR_SK,
            SRC.staging_raw_id_sk
        )
    WHEN MATCHED AND (
           ISNULL(DST.CompanyName, N'') <> ISNULL(SRC.CompanyName, N'')
        OR ISNULL(DST.Phone, N'') <> ISNULL(SRC.Phone, N'')
        OR ISNULL(DST.staging_raw_id_nk, -1) <> ISNULL(SRC.staging_raw_id_sk, -1)
        OR DST.SOR_SK <> @SOR_SK
    )
    THEN
        UPDATE SET
            DST.CompanyName = SRC.CompanyName,
            DST.Phone = SRC.Phone,
            DST.SOR_SK = @SOR_SK,
            DST.staging_raw_id_nk = SRC.staging_raw_id_sk;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
