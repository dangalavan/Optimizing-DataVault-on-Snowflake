	USE DATABASE DATA_VAULT;

	USE SCHEMA PUBLIC;

	USE WAREHOUSE COMPUTE_WH;

	ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'SMALL';

	/* Create OR REPLACE View BIZ.VW_OPENWEATHER_FORECAST
		Comment = 'Open Weather Map data'
		AS ( */
 
	SELECT S.FORECAST_H_FK 
			,S.FORECAST_ATTRIBUTES:city.country::STRING		COUNTRY_CODE
			,S.FORECAST_ATTRIBUTES:city.name::STRING		CITY
			,TO_TIMESTAMP(D.value:dt::STRING)				Weather_TIMESTAMP			

			,(D.value:temp.day::decimal(10,2) - 273.15)  	TEMPERATURE_CELCIUS_DAYTIME -- convert Kelvin to Celcius
			,(D.value:temp.min::decimal(10,2) - 273.15)  	TEMPERATURE_CELCIUS_MIN		-- convert Kelvin to Celcius
			,(D.value:temp.max::decimal(10,2) - 273.15)  	TEMPERATURE_CELCIUS_MAX		-- convert Kelvin to Celcius						
		
			,W.value:description::STRING 					WEATHER_DESCRIPTION
					
	FROM DATA_VAULT."PUBLIC".WEATHER_FORECAST_S S
		,LATERAL FLATTEN (input => S.FORECAST_ATTRIBUTES, path => 'data') D
		,LATERAL FLATTEN (INPUT => D.value:weather) W

	WHERE 	S.FORECAST_ATTRIBUTES:city.country::STRING = 'DE'
		AND S.FORECAST_ATTRIBUTES:city.name::STRING = ('Munich')
		AND S.FORECAST_MADE_DTS > (DATEADD(month,-6, CURRENT_TIMESTAMP()))
	ORDER BY S.FORECAST_MADE_DTS DESC, Weather_TIMESTAMP DESC
	--)
		; 
		
	ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL';


	-- view results
		SELECT *
		FROM DATA_VAULT.BIZ.VW_OPENWEATHER_FORECAST;


	--------------------------------------------------------------------------------------------------------------------
	------------------------------------------------------------------------------------------------------------------------
	-- Optimize
	-- 
	-- Background process automatically maintains the data in the materialzed view.
	-- Credit costs are tracked in a Snowflake-provided virtual warehouse named MATERIALIZED_VIEW_MAINTENANCE.
	------------------------------------------------------------------------------------------------------------------------
	------------------------------------------------------------------------------------------------------------------------
		
		-- 1. DDL
			CREATE OR REPLACE MATERIALIZED VIEW BIZ.VW_OPENWEATHER_FORECAST_MV 
				AS
				
				SELECT S.FORECAST_H_FK 
						,S.FORECAST_MADE_DTS
						,S.FORECAST_ATTRIBUTES:city.country::STRING		COUNTRY_CODE
						,S.FORECAST_ATTRIBUTES:city.name::STRING		CITY
						,TO_TIMESTAMP(D.value:dt::STRING)				Weather_TIMESTAMP			
			
						,(D.value:temp.day::decimal(10,2) - 273.15)  	TEMPERATURE_CELCIUS_DAYTIME -- convert Kelvin to Celcius
						,(D.value:temp.min::decimal(10,2) - 273.15)  	TEMPERATURE_CELCIUS_MIN		-- convert Kelvin to Celcius
						,(D.value:temp.max::decimal(10,2) - 273.15)  	TEMPERATURE_CELCIUS_MAX		-- convert Kelvin to Celcius						
					
						,W.value:description::STRING 					WEATHER_DESCRIPTION
				
				FROM DATA_VAULT."PUBLIC".WEATHER_FORECAST_S S
					,LATERAL FLATTEN (input => S.FORECAST_ATTRIBUTES, path => 'data') D
					,LATERAL FLATTEN (INPUT => D.value:weather) W
			
				WHERE 	S.FORECAST_ATTRIBUTES:city.country::STRING = 'DE'
					AND S.FORECAST_ATTRIBUTES:city.name::STRING = ('Munich');

			
		-- 2. View results

				SELECT *
				FROM BIZ.VW_OPENWEATHER_FORECAST_MV MV
				WHERE MV.FORECAST_MADE_DTS > (DATEADD(month,-6, CURRENT_TIMESTAMP()))
				ORDER BY MV.FORECAST_MADE_DTS DESC
						,MV.Weather_TIMESTAMP DESC;		
			
				
				