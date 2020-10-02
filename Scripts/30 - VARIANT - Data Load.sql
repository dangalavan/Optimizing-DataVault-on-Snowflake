	USE DATABASE SNOWFLAKE_SAMPLE_DATA;
	USE SCHEMA WEATHER;
	SET REC_SRC = 'SNOWFLAKE_SAMPLE_DATABASE_WEATHER';

	/*
	 --RESET
	 
		 TRUNCATE TABLE DATA_VAULT.PUBLIC.WEATHER_FORECAST_H;
		 TRUNCATE TABLE DATA_VAULT.PUBLIC.WEATHER_FORECAST_S;	  

	 */
	
	-- https://openweathermap.org/forecast16#JSON


		USE WAREHOUSE COMPUTE_WH;
		
		ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'MEDIUM';
		
		INSERT ALL

		----------------------------------			
		-- Hub - Weather Forecast			
		----------------------------------
			WHEN (SELECT COUNT(*)        
					 FROM DATA_VAULT.PUBLIC.WEATHER_FORECAST_H H        
					 WHERE H.FORECAST_PK = PK) = 0 	
            THEN 
				INTO DATA_VAULT.PUBLIC.WEATHER_FORECAST_H(FORECAST_BK,FORECAST_PK,LOAD_DTS,REC_SRC)			
				VALUES (BK, PK, LOAD_DTS, REC_SRC)

		----------------------------------
		-- Sat - Weather Forecast	
		----------------------------------	
			WHEN (SELECT COUNT(*)        
					 FROM DATA_VAULT.PUBLIC.WEATHER_FORECAST_S S        
					 WHERE S.HASH_DIFF = HASH_DIFF) = 0 
			THEN		 
				INTO DATA_VAULT.PUBLIC.WEATHER_FORECAST_S (FORECAST_H_FK,LOAD_DTS,FORECAST_MADE_DTS, FORECAST_ATTRIBUTES,HASH_DIFF,REC_SRC)
				VALUES (PK, LOAD_DTS, FORECAST_MADE_DTS, VARIANT_PAYLOAD, HASH_DIFF, REC_SRC)	

		------------------------------		
		-- Source  ("Staging")	
		------------------------------		
			SELECT (W.T::STRING || '-' || W.V:city.id::STRING) 	AS BK -- Concatenate JSON timestamp and City ID
					,MD5(BK)						AS PK -- Add preferred Hashing approach				
					,CURRENT_TIMESTAMP() 			AS LOAD_DTS	
					,w.T 							AS FORECAST_MADE_DTS
					,w.V							AS VARIANT_PAYLOAD
					,MD5(w.V)						AS HASH_DIFF
					,$REC_SRC						AS REC_SRC				
			FROM  "SNOWFLAKE_SAMPLE_DATA"."WEATHER"."DAILY_14_TOTAL" W;	
			
		
		ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL';		