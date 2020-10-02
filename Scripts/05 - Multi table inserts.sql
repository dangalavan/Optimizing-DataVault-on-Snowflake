	-------------------------------------------------
	-- MULTI-TABLE INSERTS
	-------------------------------------------------
	
	USE DATABASE SNOWFLAKE_SAMPLE_DATA;
	USE SCHEMA TPCH_SF1;

	SET REC_SRC = 'SNOWFLAKE_SAMPLE_DATABASE';


	-- Insert into all tables within the same transation
		-- With Overwrite, Truncation occurs within the same transaction aswell.

-------------------------------------
------- Supplier Hub & Sat
-------------------------------------
		INSERT OVERWRITE ALL 
			INTO DATA_VAULT."PUBLIC".SUPPLIER_H
			VALUES (PK,S_SUPPKEY,CT,RS)
					
			INTO DATA_VAULT."PUBLIC".SUPPLIER_S	(SUPPLIER_H_FK, LOAD_DTS, NAME, ADDRESS, PHONE, ACCTBAL, NATIONCODE, HASH_DIFF, REC_SRC)
			VALUES (PK,CT,S_NAME,S_ADDRESS,S_PHONE,S_ACCTBAL,S_NATIONKEY,HASH_DIFF,RS)
								
			SELECT 	MD5(S.S_SUPPKEY) AS PK  -- Add preferred Hashing approach 
					,S.S_SUPPKEY
					,CURRENT_TIMESTAMP() 	CT
					,S_NAME 
					,S_ADDRESS 
					,S_PHONE
					,S_ACCTBAL 
					,S_NATIONKEY 
					,MD5(S_NAME || S_ADDRESS || S_PHONE || S_ACCTBAL || S_NATIONKEY) HASH_DIFF -- Add preferred Hashing approach
					,$REC_SRC				RS
			FROM SUPPLIER S;
	
		
		
 --------------------------------
 -- Part Hub & Sat
 --------------------------------
 
		INSERT OVERWRITE ALL 
			
			-- Hub PART_H
				INTO DATA_VAULT."PUBLIC".PART_H
				VALUES (PK,P_PARTKEY,CT,RS)
				
			-- Sat PART_S	
				INTO DATA_VAULT."PUBLIC".PART_S	(PART_H_FK, LOAD_DTS, PART_NAME, PART_MANUFACTURER, PART_BRAND, PART_TYPE, PART_SIZE, PART_CONTAINER, PART_RETAIL_PRICE,  HASH_DIFF, REC_SRC)
				VALUES (PK,CT,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,HASH_DIFF,RS)
			
			-- Source		
			SELECT 	MD5(P.P_PARTKEY) AS PK 	-- Add preferred Hashing approach 
					,P.P_PARTKEY
					,CURRENT_TIMESTAMP() 	CT					
					,P.P_NAME 
					,P_MFGR 
					,P_BRAND 
					,P_TYPE 
					,P_SIZE 
					,P_CONTAINER 
					,P_RETAILPRICE 
					
					,MD5(P.P_NAME || P_MFGR || P_BRAND || P_TYPE || P_SIZE || P_CONTAINER || P_RETAILPRICE ) HASH_DIFF -- Add preferred Hashing approach
					,$REC_SRC				RS
				--,'' RS
			FROM PART P			
			;

 --------------------------------
 ---- Inventory Hub, Sat, Link
 --------------------------------
 
			INSERT OVERWRITE ALL 
			INTO DATA_VAULT."PUBLIC".SUPPLIER_INVENTORY_H
			VALUES (INVENTORY_PK,INVENTORY_BK,CT,RS)

			INTO DATA_VAULT."PUBLIC".SUPPLIER_INVENTORY_S	(INVENTORY_H_PK, LOAD_DTS, SUPPLY_COST, AVAILABLE_QTY, PART_BK, SUPPLIER_BK, HASH_DIFF, REC_SRC)
			VALUES (INVENTORY_PK,CT,PS_SUPPLYCOST,PS_AVAILQTY,PS_PARTKEY,PS_SUPPKEY,HASH_DIFF,RS)					
					
			INTO DATA_VAULT."PUBLIC".SUPPLIER_INVENTORY_L	(SUPPLIER_INVENTORY_L_PK, PART_PK, SUPPLIER_PK, INVENTORY_PK, LOAD_DTS, REC_SRC)
			VALUES (INVENTORY_L_PK,PART_PK,SUPPLIER_PK,INVENTORY_PK,CT,RS)

								
			SELECT 	PS_PARTKEY || PS_SUPPKEY 	 					AS INVENTORY_BK
					,MD5(INVENTORY_BK) 								AS INVENTORY_PK 	-- Add preferred Hashing approach
					,MD5(PS_PARTKEY) 			 					AS PART_PK			-- Add preferred Hashing approach
					,MD5(PS_SUPPKEY) 			 					AS SUPPLIER_PK		-- Add preferred Hashing approach
					,MD5(PS_PARTKEY || PS_SUPPKEY || INVENTORY_BK) 	AS INVENTORY_L_PK					
					,PS_AVAILQTY
					,PS_SUPPLYCOST
					,PS_PARTKEY
					,PS_SUPPKEY					
					,MD5(PS_AVAILQTY || PS_SUPPLYCOST) HASH_DIFF	-- Add preferred Hashing approach
					,CURRENT_TIMESTAMP() 	CT					
					,$REC_SRC				RS
					--,'' RS
			FROM PARTSUPP P;