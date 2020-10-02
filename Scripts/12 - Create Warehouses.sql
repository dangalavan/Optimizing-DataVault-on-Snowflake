
	/*
	 * 
	 * Scaling policy for a multi-cluster warehouse : only applies if it is running in --> Auto-scale <-- mode
	 *  Maximized mode: all clusters run concurrently so there is no need to start or shut down individual clusters
	 *  
	 */
	
-- Virtual warehouse (cluster) for Hubs
	CREATE OR REPLACE WAREHOUSE WH_ETL_HUBS 
		WITH WAREHOUSE_SIZE = 'XSMALL' 				-- XSMALL | SMALL | MEDIUM | LARGE | XLARGE | XXLARGE | XXXLARGE | X4LARGE
				AUTO_SUSPEND = 60					-- Seconds 
				AUTO_RESUME = TRUE 
				MIN_CLUSTER_COUNT = 1 
				MAX_CLUSTER_COUNT = 2 
				SCALING_POLICY = 'STANDARD';		-- Or Economy.. Additional cluster starts....
													-- ....only if the system estimates there’s enough query load to keep the cluster 
                                                    -- busy for at least 6 minutes
			
-- Virtual warehouse (cluster) for Links			
	CREATE OR REPLACE WAREHOUSE WH_ETL_LINKS 
		WITH WAREHOUSE_SIZE = 'XSMALL' 
				AUTO_SUSPEND = 60
				AUTO_RESUME = TRUE 
				MIN_CLUSTER_COUNT = 1 
				MAX_CLUSTER_COUNT = 2 
				SCALING_POLICY = 'STANDARD';
				
-- Virtual warehouse (cluster) for Sats			
	CREATE OR REPLACE WAREHOUSE WH_ETL_SATS 
		WITH WAREHOUSE_SIZE = 'XSMALL' 
				AUTO_SUSPEND = 60
				AUTO_RESUME = TRUE 
				MIN_CLUSTER_COUNT = 1 
				MAX_CLUSTER_COUNT = 2 
				SCALING_POLICY = 'STANDARD';			