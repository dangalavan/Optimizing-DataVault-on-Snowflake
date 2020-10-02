	/*
	 *  -- RESET
	  
	  TRUNCATE TABLE DATA_VAULT."PUBLIC".SUPPLIER_INVENTORY_L;
	 */

	USE DATABASE SNOWFLAKE_SAMPLE_DATA;

	USE SCHEMA TPCH_SF1;

	SET REC_SRC = 'SNOWFLAKE_SAMPLE_DATABASE';

	USE WAREHOUSE WH_ETL_LINKS; 


 --------------------------------
 ---- Inventory Link
 --------------------------------


		INSERT INTO DATA_VAULT."PUBLIC".SUPPLIER_INVENTORY_L	
				(
				SUPPLIER_INVENTORY_L_PK
				, PART_PK
				, SUPPLIER_PK
				, INVENTORY_PK
				, LOAD_DTS
				, REC_SRC
				)
				
		WITH cte_InvLink
			AS
			(
			SELECT 	(PS_PARTKEY || PS_SUPPKEY) 						AS INVENTORY_BK -- Add preferred Hashing approach
					,MD5(PS_PARTKEY || PS_SUPPKEY || INVENTORY_BK) 	AS SUPPLIER_INVENTORY_L_PK
					,CURRENT_TIMESTAMP() 	CT					
					
					,MD5(INVENTORY_BK) 								AS INVENTORY_PK -- Add preferred Hashing approach								
					,MD5(PS_PARTKEY) 			 					AS PART_PK		-- Add preferred Hashing approach
					,MD5(PS_SUPPKEY) 			 					AS SUPPLIER_PK	-- Add preferred Hashing approach
					
					,$REC_SRC				RS
					--,'' RS
					
			FROM PARTSUPP P
			WHERE SUPPLIER_INVENTORY_L_PK NOT IN (SELECT L.SUPPLIER_INVENTORY_L_PK FROM DATA_VAULT."PUBLIC".SUPPLIER_INVENTORY_L L)
			)
		

		SELECT SUPPLIER_INVENTORY_L_PK
				,PART_PK
				,SUPPLIER_PK
				,INVENTORY_PK
				,CT
				,RS
		FROM cte_InvLink;
		
		
		
		