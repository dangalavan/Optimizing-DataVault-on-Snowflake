CREATE OR REPLACE DATABASE DATA_VAULT;

USE DATA_VAULT.PUBLIC;

-- *********************** SqlDBM: Snowflake ************************
-- ******************************************************************


CREATE SCHEMA IF NOT EXISTS "BIZ";

USE DATA_VAULT.PUBLIC;



-- ************************************** "WEATHER_FORECAST_H"
CREATE TABLE IF NOT EXISTS "WEATHER_FORECAST_H"
(
 "FORECAST_PK" varchar NOT NULL,
 "FORECAST_BK" varchar NOT NULL,
 "LOAD_DTS"    timestamp NOT NULL,
 "REC_SRC"     string NOT NULL,
 CONSTRAINT "PK_weather_forecast_h" PRIMARY KEY ( "FORECAST_PK" ),
 CONSTRAINT "AK_FORECAST_BK" UNIQUE ( "FORECAST_BK" )
);


-- ************************************** "SUPPLIER_INVENTORY_H"
CREATE TABLE IF NOT EXISTS "SUPPLIER_INVENTORY_H"
(
 "INVENTORY_PK" varchar NOT NULL,
 "INVENTORY_BK" varchar NOT NULL,
 "LOAD_DTS"     timestamp NOT NULL,
 "REC_SRC"      varchar NOT NULL,
 CONSTRAINT "PK_inventory_h" PRIMARY KEY ( "INVENTORY_PK" ),
 CONSTRAINT "AK_SUPPLIER_INVENTORY_H" UNIQUE ( "INVENTORY_BK" )
);


-- ************************************** "SUPPLIER_H"
CREATE TABLE IF NOT EXISTS "SUPPLIER_H"
(
 "SUPPLIER_PK" varchar NOT NULL,
 "SUPPLIER_BK" varchar NOT NULL,
 "LOAD_DTS"    timestamp NOT NULL,
 "REC_SRC"     varchar NOT NULL,
 CONSTRAINT "PK_supplier_h" PRIMARY KEY ( "SUPPLIER_PK" ),
 CONSTRAINT "AK_SUPPLIER_H" UNIQUE ( "SUPPLIER_BK" )
);


-- ************************************** "PART_H"
CREATE TABLE IF NOT EXISTS "PART_H"
(
 "PART_PK"  varchar NOT NULL,
 "PART_BK"  varchar NOT NULL,
 "LOAD_DTS" timestamp NOT NULL,
 "REC_SRC"  varchar NOT NULL,
 CONSTRAINT "PK_part_h" PRIMARY KEY ( "PART_PK" ),
 CONSTRAINT "AK_PART_H" UNIQUE ( "PART_BK" )
);


-- ************************************** "WEATHER_FORECAST_S"
CREATE TABLE IF NOT EXISTS "WEATHER_FORECAST_S"
(
 "FORECAST_H_FK"       varchar NOT NULL,
 "LOAD_DTS"            timestamp NOT NULL,
 "FORECAST_MADE_DTS"   timestamp NOT NULL,
 "FORECAST_ATTRIBUTES" variant NOT NULL,
 "HASH_DIFF"           varchar NOT NULL,
 "REC_SRC"             varchar NOT NULL,
 CONSTRAINT "PK_weather_forecast_s" PRIMARY KEY ( "FORECAST_H_FK", "LOAD_DTS" ),
 CONSTRAINT "Forecast_rel" FOREIGN KEY ( "FORECAST_H_FK" ) REFERENCES "WEATHER_FORECAST_H" ( "FORECAST_PK" )
);


-- ************************************** "SUPPLIER_S"
CREATE TABLE IF NOT EXISTS "SUPPLIER_S"
(
 "SUPPLIER_H_FK" varchar NOT NULL,
 "LOAD_DTS"      timestamp NOT NULL,
 "NAME"          varchar,
 "ADDRESS"       varchar,
 "PHONE"         varchar,
 "ACCTBAL"       varchar,
 "NATIONCODE"    varchar,
 "HASH_DIFF"     varchar NOT NULL,
 "REC_SRC"       varchar NOT NULL,
 CONSTRAINT "PK_supplier_s" PRIMARY KEY ( "SUPPLIER_H_FK", "LOAD_DTS" ),
 CONSTRAINT "FK_S_H_S_SAT" FOREIGN KEY ( "SUPPLIER_H_FK" ) REFERENCES "SUPPLIER_H" ( "SUPPLIER_PK" )
);


-- ************************************** "SUPPLIER_INVENTORY_S"
CREATE TABLE IF NOT EXISTS "SUPPLIER_INVENTORY_S"
(
 "INVENTORY_H_PK" varchar NOT NULL,
 "LOAD_DTS"       timestamp NOT NULL,
 "SUPPLY_COST"    number(12,2),
 "AVAILABLE_QTY"  number(38,0),
 "PART_BK"        varchar NOT NULL,
 "SUPPLIER_BK"    varchar NOT NULL,
 "HASH_DIFF"      varchar NOT NULL,
 "REC_SRC"        varchar NOT NULL,
 CONSTRAINT "PK_supplier_inventory_s" PRIMARY KEY ( "INVENTORY_H_PK", "LOAD_DTS" ),
 CONSTRAINT "FK_I_SUP_INV" FOREIGN KEY ( "INVENTORY_H_PK" ) REFERENCES "SUPPLIER_INVENTORY_H" ( "INVENTORY_PK" )
);


-- ************************************** "SUPPLIER_INVENTORY_L"
CREATE TABLE IF NOT EXISTS "SUPPLIER_INVENTORY_L"
(
 "SUPPLIER_INVENTORY_L_PK" varchar NOT NULL,
 "PART_PK"                 varchar NOT NULL,
 "SUPPLIER_PK"             varchar NOT NULL,
 "INVENTORY_PK"            varchar NOT NULL,
 "LOAD_DTS"                varchar NOT NULL,
 "REC_SRC"                 varchar NOT NULL,
 CONSTRAINT "PK_supplier_inventory_l" PRIMARY KEY ( "SUPPLIER_INVENTORY_L_PK" ),
 CONSTRAINT "AK_S_I" UNIQUE ( "SUPPLIER_PK", "INVENTORY_PK", "PART_PK" ),
 CONSTRAINT "FK_PART_TO_SUPPLIER_INVENTORY" FOREIGN KEY ( "PART_PK" ) REFERENCES "PART_H" ( "PART_PK" ),
 CONSTRAINT "FK_S_I" FOREIGN KEY ( "INVENTORY_PK" ) REFERENCES "SUPPLIER_INVENTORY_H" ( "INVENTORY_PK" ),
 CONSTRAINT "FK_S_SUP_INV" FOREIGN KEY ( "SUPPLIER_PK" ) REFERENCES "SUPPLIER_H" ( "SUPPLIER_PK" )
);


-- ************************************** "PART_S"
CREATE TABLE IF NOT EXISTS "PART_S"
(
 "PART_H_FK"         varchar NOT NULL,
 "LOAD_DTS"          timestamp NOT NULL,
 "PART_NAME"         varchar,
 "PART_MANUFACTURER" varchar,
 "PART_BRAND"        varchar,
 "PART_TYPE"         varchar,
 "PART_SIZE"         number(10),
 "PART_CONTAINER"    varchar,
 "PART_RETAIL_PRICE" number(12,2),
 "HASH_DIFF"         varchar NOT NULL,
 "REC_SRC"           varchar NOT NULL,
 CONSTRAINT "PK_part_s" PRIMARY KEY ( "PART_H_FK", "LOAD_DTS" ),
 CONSTRAINT "FK_PART_H" FOREIGN KEY ( "PART_H_FK" ) REFERENCES "PART_H" ( "PART_PK" )
)
COMMENT = 'The Parts Satellite.';


-- ************************************** "BIZ"."VW_OPENWEATHER_FORECAST_MV"
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

-- ************************************** "BIZ"."VW_OPENWEATHER_FORECAST"
Create OR REPLACE View BIZ.VW_OPENWEATHER_FORECAST

  Comment = 'Open Weather Map data'

AS
(
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
);

