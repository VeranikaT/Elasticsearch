package com.trafimchuk.veranika.jai;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElasticSearchRun {
	
	private static final Logger logger = LogManager.getLogger( ElasticSearchRun.class.getName() );
	
	private static String NUMBER_OF_SHARDS  = "number_of_shards";
	private static String NUMBER_OF_REPLICAS= "number_of_replicas";
	private static String CLUSTER_NAME 		= "cluster_name";
	private static String INDEX_NAME 		= "index_name";
	private static String INDEX_TYPE		= "index_type";
	private static String IP  = "master_ip";
	private static String PORT= "master_port";
	
	public static void main( String[] args ) 
    {
		logger.info( "Starting..." );
		PropertyReader properties = null;
		ESJavaApi es = null;
		
		try {
			properties = new PropertyReader( getRelativeResourcePath( "config.properties" ) );
			
			String numberOfShards  	= properties.read( NUMBER_OF_SHARDS );
			String numberOfReplicas	= properties.read( NUMBER_OF_REPLICAS );
			
			String clusterName 		= properties.read( CLUSTER_NAME );
			
			String indexName 		= properties.read( INDEX_NAME );
			String indexType 		= properties.read( INDEX_TYPE );

			String ip 				= properties.read( IP );
			int    port				= Integer.parseInt( properties.read( PORT ) );
		
			es = new ESJavaApi( clusterName, ip, port );
			
			es.isClusterHealthy();
			
			if( !es.isIndexRegistered( indexName ) ) {
				//create index
				es.createIndex( indexName, numberOfShards, numberOfReplicas );

				//insert mapping to the index
				es.bulkInsert( indexName, indexType );

				//insert some test data (from JSON file)
				// es.bulkInsert( indexName, indexType, getRelativeResourcePath( "data.json" ) );
			}
			else{

				logger.info("WE ARE SEARCHING BY FILTER :");

				//search all events
				es.queryResultsAllEvents( indexName, "events" );

				//search workshop events only
				es.queryResultsWithEventTypeFilter( indexName, "workshop" );

				//search by two filters
				es.queryResultsWithFewFilters(indexName, "Title6", "Poland");
				//delete all events
				es.delete(indexName, "_index", "events");

			}

		}
		catch ( FileNotFoundException e ) {
			e.printStackTrace();
		}
		catch ( UnknownHostException e ) {
			e.printStackTrace();
		}
		catch ( IOException e ) {
			e.printStackTrace();
		}
		finally {
			es.close();
		}
    }
	
	private static String getRelativeResourcePath( String resource ) throws FileNotFoundException {
		
		if( resource == null || resource.equals("") ) throw new IllegalArgumentException( resource );
		
		URL url = ElasticSearchRun.class.getClassLoader().getResource( resource );
		
		if( url == null ) throw new FileNotFoundException( resource );
		
		return url.getPath();
	}

}
