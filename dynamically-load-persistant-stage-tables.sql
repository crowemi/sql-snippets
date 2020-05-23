CREATE PROCEDURE [psa].[SHS_SP_PSA_LOAD_TABLE] (
	@LogicalId INT 
)
AS
BEGIN 

	SET NOCOUNT ON;

	-- =============================================
	-- Create date: 06/10/2019
	-- Description:	This stored procedure is used to generically migrate records from stage to persistant stage. 
	-- =============================================
	-- Change log
	-- =============================================
	-- 06/10/2019	MAC - Initial Draft.
	-- 08/01/2019	MAC - Updated script to handle compressed columns.
	-- 08/14/2019	MAC - Updated script to handle PSA change records, add a record to PSA when the record hash differs from the previous version.
	-- 10/28/2019	MAC - Updated script to handle batch processing.
    -- 05/23/2020   MAC - Updated to set IS_CURRENT = 0 for replace records
	-- =============================================

    -- TODO:
    --      Make database references dynamic.
	
    -- NOTE:	If the PSA table has a compressed column and source columns must be identified within the 
	--			table definition in the form of extended properties on the target column; 
	--				IS_COMPRESSED
	--				COMPRESSED_SOURCE_COLUMN

	DECLARE @stgColumns TABLE ( 
		ColumnId INT NOT NULL IDENTITY(1,1),
		ColumnName VARCHAR(MAX) NOT NULL
	);

	DECLARE @psaColumns TABLE (
		ColumnId INT NOT NULL IDENTITY(1,1),
		ColumnName VARCHAR(MAX) NOT NULL,
		IsCompressed BIT NULL,
		CompressedColumn VARCHAR(MAX) NULL,
		Ignore BIT NULL
	);


	DECLARE @sql NVARCHAR(MAX);

	--Batch variables 
	--TODO: Make @BatchSize an input parameter into stored procedure
	DECLARE @BatchSize INT = 500000; 
	DECLARE @BatchIterator INT = 1; 
	DECLARE @BatchCount INT = 0;
	DECLARE @BatchRowCount INT = 0;

	--PSA variables
	DECLARE @psaTableDatabase VARCHAR(MAX),
			@psaTableSchema VARCHAR(MAX),
			@psaTableName VARCHAR(MAX),
			@psaHpxrUidExists BIT = 0, 
			@psaRecordUidExists BIT = 0, 
			@psaRecordHashExists BIT = 0, 
			@psaChangeDtExists BIT = 0,
			@psaColumnCount INT = 0,
			@psaColumnIterator INT = 1,
            @psaIsCurrentExists BIT = 0; -- flag used to ensure IS_CURRENT column exists on PSA table

	--STG variables
	DECLARE @stgTableDatabase VARCHAR(MAX);
	DECLARE @stgTableSchema VARCHAR(MAX);
	DECLARE @stgTableName VARCHAR(MAX);
	DECLARE @stgColumnCount INT = 0;
	DECLARE @stgColumnIterator INT = 1;

	SELECT 
		@psaTableDatabase = PSA_DATABASE_NAME,
		@psaTableName = PSA_TABLE_NAME,
		@psaTableSchema = PSA_SCHEMA_NAME,
		@stgTableDatabase = STG_DATABASE_NAME,
		@stgTableName = STG_TABLE_NAME,
		@stgTableSchema = STG_SCHEMA_NAME
	FROM hpXr_Stage.dbo.TABLE_METADATA 
	WHERE LOGICAL_ID = @LogicalId	

	-- Insert the columns from the system table for the STG table in question
	INSERT INTO @stgColumns
	SELECT 
		stgColumns.name
	FROM hpXr_Stage.sys.columns stgColumns 
		JOIN (
			--This join will only add columns to the stage columns table that are also columns on the psa table
			SELECT 
				name
			FROM hpXr_Stage.sys.columns 
			WHERE OBJECT_ID = OBJECT_ID(CONCAT(@psaTableDatabase, '.', @psaTableSchema, '.', @psaTableName))
		) psaColumns ON psaColumns.name = stgColumns.name
	WHERE OBJECT_ID = OBJECT_ID(CONCAT(@stgTableDatabase, '.', @stgTableSchema, '.', @stgTableName));

	-- Insert the columns from the system table for the PSA table in question
	INSERT INTO @psaColumns 
	SELECT 
		c.name,
		CAST(isCompressed.value AS BIT) IS_COMPRESSED,
		CAST(compressedSourceColumn.value AS VARCHAR(MAX)) COMPRESSED_SOURCE_COLUMN,
		CASE WHEN stg.ColumnName IS NULL THEN 1 ELSE 0 END
	FROM hpXr_Stage.sys.columns c
		OUTER APPLY (
			-- Check each column for extended property COMPRESSED_SOURCE_COLUMN
			-- to determine the source column name to be compressed into the target column.
			SELECT 
				e.value 
			FROM sys.extended_properties e
			WHERE e.NAME = 'COMPRESSED_SOURCE_COLUMN'
				AND e.major_id = c.object_id
				AND e.minor_id = c.column_id
		) compressedSourceColumn 

		OUTER APPLY (
			-- Check each column for extended property IS_COMPRESSED to identify if column
			-- should be inserted with compressed data.
			SELECT 
				e.value 
			FROM sys.extended_properties e
			WHERE e.NAME = 'IS_COMPRESSED'
				AND e.major_id = c.object_id
				AND e.minor_id = c.column_id
		) isCompressed 

		LEFT JOIN @stgColumns stg ON stg.ColumnName = c.name

	WHERE c.OBJECT_ID = OBJECT_ID(CONCAT(@psaTableDatabase, '.', @psaTableSchema, '.', @psaTableName))
		AND C.is_computed = 0;

	SELECT @stgColumnCount = COUNT(1) FROM @stgColumns;
	SELECT @psaColumnCount = COUNT(1) FROM @psaColumns;

	-- Check that required fields exist
	SELECT @psaHpxrUidExists = COUNT(1) FROM @psaColumns WHERE ColumnName = 'HPXR_UID';
	SELECT @psaRecordUidExists = COUNT(1) FROM @psaColumns WHERE ColumnName = 'RECORD_UID';
	SELECT @psaRecordHashExists = COUNT(1) FROM @psaColumns WHERE ColumnName = 'RECORD_HASH';
	SELECT @psaChangeDtExists = COUNT(1) FROM @psaColumns WHERE ColumnName = 'CHANGE_DT';
	SELECT @psaIsCurrentExists = COUNT(1) FROM @psaColumns WHERE ColumnName = 'IS_CURRENT';

	PRINT '@psaHpxrUidExists: ' + CAST(@psaHpxrUidExists AS VARCHAR(1));
	PRINT '@psaRecordUidExists: ' + CAST(@psaRecordUidExists AS VARCHAR(1));
	PRINT '@psaRecordHashExists: ' + CAST(@psaRecordHashExists AS VARCHAR(1));
	PRINT '@psaChangeDtExists: ' + CAST(@psaChangeDtExists AS VARCHAR(1));
	PRINT '@psaIsCurrentExists: ' + CAST(@psaIsCurrentExists AS VARCHAR(1));

	-- Perform check to make sure required fields exist
	IF(@psaHpxrUidExists = 0 OR @psaRecordUidExists = 0 OR @psaRecordHashExists = 0 OR @psaChangeDtExists = 0)
		THROW 51000, 'The persistant stage table is missing one or more of the required administrative columns (HPXR_UID, RECORD_UID, RECORD_HASH, CHANGE_DT).', 1 

	--TODO: Change the schema from dbo to tmp
	--Create a listing of Row Number and Primary key for batch processing STG table
	SET @sql = 'DROP TABLE IF EXISTS dbo.TEMP_' + @stgTableName + '
	
				SELECT 
					DENSE_RANK() OVER(ORDER BY HPXR_UID ASC) RowNum,
					HPXR_UID
				INTO dbo.TEMP_' + @stgTableName + '
				FROM ' + @stgTableDatabase + '.' + @stgTableSchema + '.' + @stgTableName + '

				CREATE CLUSTERED COLUMNSTORE INDEX TEMP_' + @stgTableName  + '_CCI ON dbo.TEMP_' + @stgTableName;

	EXEC hpXr_Stage.sys.sp_executesql @sql;

	-- Get record count
	SET @sql = 'SELECT @BatchCount = COUNT(1) FROM dbo.TEMP_' + @stgTableName;

	EXEC hpXr_Stage.sys.sp_executesql @sql, N'@BatchCount INT OUTPUT', @BatchCount OUTPUT;

	--Create a listing of Row Number and  Primary Key for batch processing PSA table
	SET @sql = 'DROP TABLE IF EXISTS dbo.TEMP_' + @psaTableName + '
				
				SELECT 
					ROW_NUMBER() OVER (PARTITION BY RECORD_UID ORDER BY RECORD_UID ASC, CHANGE_DT ASC) RowNum,
					RECORD_UID,
					RECORD_HASH
				INTO dbo.TEMP_' + @psaTableName + '
				FROM ' + @psaTableDatabase + '.' + @psaTableSchema + '.' + @psaTableName + '
		
				CREATE CLUSTERED COLUMNSTORE INDEX TEMP_' + @psaTableName  + '_CCI ON dbo.TEMP_' + @psaTableName;

	EXEC hpXr_Stage.sys.sp_executesql @sql;


	WHILE @BatchIterator <= @BatchCount
	BEGIN 

		SET @Sql = '';

		PRINT '-----'
		PRINT 'BATCH START'
		PRINT '-----'

		PRINT '@Set Begin: ' + CAST(@BatchIterator AS VARCHAR);
		PRINT '@Set End: ' + CAST((@BatchSize + @BatchIterator) AS VARCHAR); 
		PRINT '@BatchCount: ' + CAST(@BatchCount AS VARCHAR);

 		SET @sql = @sql + ' INSERT INTO psa.' + @psaTableName + ' ( ';

		WHILE @psaColumnIterator <= @psaColumnCount 
		BEGIN 

			DECLARE @psaCurrentColumn VARCHAR(MAX);
			DECLARE @psaCurrentColumnIgnore BIT;

			SELECT 
				@psaCurrentColumn = psa.ColumnName,
				@psaCurrentColumnIgnore = psa.Ignore
			FROM @psaColumns psa 
			WHERE psa.ColumnId = @psaColumnIterator
		
			IF @psaCurrentColumnIgnore = 0 
			BEGIN 
				-- if the ignore flag is not set, add column to statement
				SET @sql = @sql + @psaCurrentColumn;

				IF @psaColumnCount <> @psaColumnIterator
				BEGIN 
					SET @sql = @sql + ', ';
				END 

			END

			SET @psaColumnIterator = @psaColumnIterator + 1;

		END 

		--Reset column iterator for next batch.
		SET @psaColumnIterator = 1;

		SET @sql = @sql + ' ) SELECT '

		WHILE @stgColumnIterator <= @stgColumnCount 
		BEGIN 

			DECLARE @currentColumn VARCHAR(MAX); 
			DECLARE @currentColumnCompressed BIT;

			SELECT 
				@currentColumn = stg.ColumnName,
				@currentColumnCompressed = CASE WHEN psa.ColumnId IS NOT NULL THEN 1 ELSE 0 END 
			FROM @stgColumns stg
				LEFT JOIN @psaColumns psa ON psa.CompressedColumn = stg.ColumnName
					AND psa.IsCompressed = 1  
			WHERE stg.ColumnId = @stgColumnIterator
		
			IF @currentColumnCompressed = 1 
			BEGIN 
			
				SET @sql = @sql + 'COMPRESS(';

			END 

			SET @sql = @sql + 'stg.' + @currentColumn;

			IF @currentColumnCompressed = 1 
			BEGIN 
			
				SET @sql = @sql + ')';

			END 

			IF @stgColumnCount <> @stgColumnIterator
			BEGIN 
				SET @sql = @sql + ', ';
			END 

			SET @stgColumnIterator = @stgColumnIterator + 1;

		END 

		--Reset column iterator for next batch.
		SET @stgColumnIterator = 1;

		SET @sql = @sql + ' FROM ' + @stgTableDatabase + '.' + @stgTableSchema + '.' + @stgTableName + ' stg ';
		SET @sql = @sql + '		JOIN dbo.TEMP_' + @stgTableName + ' stgTemp ON stgTemp.HPXR_UID = stg.HPXR_UID AND stgTemp.RowNum >= ' + CAST(@BatchIterator AS VARCHAR) + ' AND stgTemp.RowNum < ' + CAST((@BatchSize + @BatchIterator) AS VARCHAR);
		SET @sql = @sql + '		LEFT JOIN ( SELECT psa.RowNum, psa.RECORD_UID, psa.RECORD_HASH FROM dbo.TEMP_' + @psaTableName + ' psa JOIN ( SELECT MAX(psaInner.RowNum) RowNum, psaInner.RECORD_UID FROM dbo.TEMP_' + @psaTableName + ' psaInner GROUP BY psaInner.RECORD_UID ) maxRecord ON maxRecord.RowNum = psa.RowNum AND maxRecord.RECORD_UID = psa.RECORD_UID ) psa ON psa.RECORD_UID = stg.RECORD_UID AND psa.RECORD_HASH = stg.RECORD_HASH ';     
		SET @sql = @sql + 'WHERE psa.RECORD_UID IS NULL;'; --Only records that don't currently exist in the PSA table
	 
		SET @sql = @sql + 'SET @BatchRowCount = @@ROWCOUNT';

		PRINT @sql

		EXEC hpXr_Stage.sys.sp_executesql @sql, N'@BatchRowCount INT OUTPUT', @BatchRowCount OUTPUT

		PRINT 'Records Inserted: ' + CAST(@BatchRowCount AS VARCHAR);

		SET @BatchIterator = @BatchIterator + @BatchSize;

		PRINT '-----'
		PRINT 'BATCH END'
		PRINT '-----'
		PRINT ''

	END

    -- if the IS_CURRENT flag exists on PSA process new records
    IF @psaIsCurrentExists = 1 
    BEGIN 
        DECLARE @is_current_update_count INT;
        DECLARE @is_current_sql NVARCHAR(MAX) = 'UPDATE psa SET psa.IS_CURRENT = 0 FROM ' + @stgTableDatabase + '.' + @stgTableSchema + '.' + @stgTableName + ' stg JOIN ' + @psaTableDatabase + '.' + @psaTableSchema + '.' + @psaTableName + ' psa ON psa.RECORD_UID = stg.RECORD_UID WHERE stg.RECORD_HASH <> psa.RECORD_HASH;';
		
        SET @is_current_sql = @is_current_sql + 'SET @is_current_update_count = @@ROWCOUNT'

        EXEC hpXr_Stage.sys.sp_executesql @is_current_sql, N'@is_current_update_count INT OUTPUT', @is_current_update_count OUTPUT
        PRINT 'Records updated: ' + CAST(@is_current_update_count AS VARCHAR);
    END 

	--Drop temp tables
	SET @sql = 'DROP TABLE IF EXISTS dbo.TEMP_' + @stgTableName;
	EXEC hpXr_Stage.sys.sp_executesql @sql;
	
	SET @sql = 'DROP TABLE IF EXISTS dbo.TEMP_' + @psaTableName;
	EXEC hpXr_Stage.sys.sp_executesql @sql;

END 