package com.trafimchuk.veranika.jai;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ESJavaApi {
	private static final Logger logger = LogManager.getLogger( ESJavaApi.class.getName() );
	
	private TransportClient client = null;
	
	public ESJavaApi(String clusterName, String clusterIp, int clusterPort ) throws UnknownHostException {
		
		Settings settings = Settings.builder()
				  .put( "cluster.name", clusterName )
				  .put( "client.transport.ignore_cluster_name", true )
				  .put( "client.transport.sniff", true )
				  .build();
				
				// create connection
				client = new PreBuiltTransportClient( settings ); 
				client.addTransportAddress( new TransportAddress( InetAddress.getByName( clusterIp ), clusterPort) );
				
		logger.info( "Connection " + clusterName + "@" + clusterIp + ":" + clusterPort + " established!" );		
	}
	
	public boolean isClusterHealthy() {

		final ClusterHealthResponse response = client
			    .admin()
			    .cluster()
			    .prepareHealth()
			    .setWaitForGreenStatus()
			    .setTimeout( TimeValue.timeValueSeconds( 2 ) )
			    .execute()
			    .actionGet();

		if ( response.isTimedOut() ) {
			logger.info( "The cluster is healthy part: " + response.getStatus() );
			return false;
		}

		logger.info( "The cluster is healthy: " + response.getStatus() );
		return true;
	}
	
	public boolean isIndexRegistered( String indexName ) {
		// check if index already exists
		final IndicesExistsResponse ieResponse = client
			    .admin()
			    .indices()
			    .prepareExists( indexName )
			    .get( TimeValue.timeValueSeconds( 1 ) );
			
		// index not there
		if ( !ieResponse.isExists() ) {
			return false;
		}
		
		logger.info( "Index already created!" );
		return true;
	}
	
	public boolean createIndex( String indexName, String numberOfShards, String numberOfReplicas ) {
		CreateIndexResponse createIndexResponse = 
			client.admin().indices().prepareCreate( indexName )
        	.setSettings( Settings.builder()             
                .put("index.number_of_shards", numberOfShards ) 
                .put("index.number_of_replicas", numberOfReplicas )
        	)
        	.get(); 
				
		if( createIndexResponse.isAcknowledged() ) {
			logger.info("Created Index with " + numberOfShards + " Shard(s) and " + numberOfReplicas + " Replica(s)!");
			return true;
		}
		
		return false;				
	}

	/**
	 * Bulk insert documents from a JSON array from file
	 * @param indexName name of the index
	 * @param indexType type of the index
	 * @return true if insert was successful, else false
	 * @throws IOException
	 * @throws ParseException
	 */
	public boolean bulkInsert( String indexName, String indexType ) throws IOException {

		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		LocalDateTime now = LocalDateTime.now();

		BulkRequestBuilder bulkRequest = client.prepareBulk();

		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add(
			client.prepareIndex( indexName, indexType, "1" )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
						.field( "title", "Title1" )
						.field("event_type", "workshop")
						.field( "date", dtf.format(now) )
						.field( "place", "Budapest" )
						.field("description", "Talk1")
						.field("list_of_subtopics", "Some sub1")
						.endObject()
	    ));

		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add(
			client.prepareIndex( indexName, indexType, "2" )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
						.field( "title", "Title2" )
						.field("event_type", "workshop")
						.field( "date", dtf.format(now) )
						.field( "place", "Hamburg" )
						.field("description", "Talk2")
						.field("list_of_subtopics", "Some sub2")
						.endObject()
		));

		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add(
			client.prepareIndex( indexName, indexType, "3" )
		        .setSource( XContentFactory.jsonBuilder()
	                .startObject()
						.field( "title", "Title3" )
						.field("event_type", "workshop")
						.field( "date", dtf.format(now) )
						.field( "place", "Paris" )
						.field("description", "Talk3")
						.field("list_of_subtopics", "Some sub3")
						.endObject()
		));

		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add(
				client.prepareIndex( indexName, indexType, "4" )
						.setSource( XContentFactory.jsonBuilder()
								.startObject()
								.field( "title", "Title4" )
								.field("event_type", "tech-talk")
								.field( "date", dtf.format(now) )
								.field( "place", "Madrid" )
								.field("description", "Talk4")
								.field("list_of_subtopics", "Some sub4")
								.endObject()
						));

		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add(
				client.prepareIndex( indexName, indexType, "5" )
						.setSource( XContentFactory.jsonBuilder()
								.startObject()
								.field( "title", "Title5" )
								.field("event_type", "tech-talk")
								.field( "date", dtf.format(now) )
								.field( "place", "London" )
								.field("description", "Talk5")
								.field("list_of_subtopics", "Some sub5")
								.endObject()
						));

		bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add(
				client.prepareIndex( indexName, indexType, "6" )
						.setSource( XContentFactory.jsonBuilder()
								.startObject()
								.field( "title", "Title6" )
								.field("event_type", "tech-talk")
								.field( "date", dtf.format(now) )
								.field( "place", "Poland" )
								.field("description", "Talk6")
								.field("list_of_subtopics", "Some sub6")
								.endObject()
						));


		BulkResponse bulkResponse = bulkRequest.get();
		if ( bulkResponse.hasFailures() ) {
			logger.info( "Bulk insert failed!" );
			return false;
		}

		return true;
	}

	/**
	 * Bulk insert documents from a JSON array from file
	 * @param indexName name of the index
	 * @param indexType type of the index
	 * @param dataPath path to the JSON data file
	 * @return true if insert was successful, else false
	 * @throws IOException
	 * @throws ParseException
	 */
	public boolean bulkInsert( String indexName, String indexType, String dataPath ) throws IOException, ParseException {
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		JSONParser parser = new JSONParser();
		// we know we get an array from the example data
		JSONArray jsonArray = (JSONArray) parser.parse( new FileReader( dataPath ) );

		@SuppressWarnings("unchecked")
		Iterator<JSONObject> it = jsonArray.iterator();

	    while( it.hasNext() ) {
	    	JSONObject json = it.next();
	    	logger.info( "Insert document: " + json.toJSONString() );

			bulkRequest.setRefreshPolicy( RefreshPolicy.IMMEDIATE ).add(
				client.prepareIndex( indexName, indexType )
					.setSource( json.toJSONString(), XContentType.JSON )
			);
	    }

		BulkResponse bulkResponse = bulkRequest.get();
		if ( bulkResponse.hasFailures() ) {
			logger.info( "Bulk insert failed: " + bulkResponse.buildFailureMessage() );
			return false;
		}

		return true;
	}

	/**
	 * Predefined template to query all
	 * @param indexName index name of where to execute the search query
	 */
	public void queryResultsAllEvents( String indexName, String index ) {
		SearchResponse scrollResp =
				client.prepareSearch( indexName )
						// sort order
						.addSort( FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC )
						.setScroll( new TimeValue( 60000 ) )
						.setPostFilter( QueryBuilders.matchQuery( "_index", index ) )
						.setSize( 100 ).get();

		do {
			int count = 1;
			for ( SearchHit hit : scrollResp.getHits().getHits() ) {
				Map<String,Object> res = hit.getSourceAsMap();

				for( Map.Entry<String,Object> entry : res.entrySet() ) {
					logger.info( "[" + count + "] " + entry.getKey() + " --> " + entry.getValue() );
				}
				count++;
			}

			scrollResp = client.prepareSearchScroll( scrollResp.getScrollId() ).setScroll( new TimeValue(60000) ).execute().actionGet();
			// zero hits mark the end of the scroll and the while loop.
		} while( scrollResp.getHits().getHits().length != 0 );
	}


	/**
	 * Predefined template to query event type
	 * @param indexName index name of where to execute the search query
	 */
	public void queryResultsWithEventTypeFilter( String indexName, String type ) {
		SearchResponse scrollResp =
			client.prepareSearch( indexName )
			// sort order
	        .addSort( FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC )
	        .setScroll( new TimeValue( 60000 ) )
	        .setPostFilter( QueryBuilders.matchQuery("event_type", type))
	        .setSize( 100 ).get();

		do {
			int count = 1;
		    for ( SearchHit hit : scrollResp.getHits().getHits() ) {
		    	Map<String,Object> res = hit.getSourceAsMap();

		    	for( Map.Entry<String,Object> entry : res.entrySet() ) {
		    		logger.info( "[" + count + "] " + entry.getKey() + " --> " + entry.getValue() );
		    	}
		    	count++;
		    }

		    scrollResp = client.prepareSearchScroll( scrollResp.getScrollId() ).setScroll( new TimeValue(60000) ).execute().actionGet();
		// zero hits mark the end of the scroll and the while loop.
		} while( scrollResp.getHits().getHits().length != 0 );
	}


	/**
	 * Predefined template to query event type
	 * @param indexName index name of where to execute the search query
	 */
	public void queryResultsWithFewFilters(String indexName, String title, String place ) {

		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");

		SearchResponse scrollResp =
				client.prepareSearch( indexName )
						// sort order
						.addSort( FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC )
						.setScroll( new TimeValue( 60000 ) )
						.setPostFilter( QueryBuilders.matchQuery("title", title))
						.setPostFilter( QueryBuilders.matchQuery("place", place))
						.setSize( 100 ).get();

		do {
			int count = 1;
			for ( SearchHit hit : scrollResp.getHits().getHits() ) {
				Map<String,Object> res = hit.getSourceAsMap();

				for( Map.Entry<String,Object> entry : res.entrySet() ) {
					logger.info( "[" + count + "] " + entry.getKey() + " --> " + entry.getValue() );
				}
				count++;
			}

			scrollResp = client.prepareSearchScroll( scrollResp.getScrollId() ).setScroll( new TimeValue(60000) ).execute().actionGet();
			// zero hits mark the end of the scroll and the while loop.
		} while( scrollResp.getHits().getHits().length != 0 );
	}

	/**
	 * Delete a document identified by a key value pair
	 * @param indexName name of the index where to delete
	 * @param key pair key
	 * @param value pair value
	 * @return number of deleted documents
	 */
	public long delete( String indexName, String key, String value ) {
		BulkByScrollResponse response =
				DeleteByQueryAction.INSTANCE.newRequestBuilder( client )
						.filter( QueryBuilders.matchQuery( key, value ) )
						.source( indexName )
						.refresh( true )
						.get();

		logger.info( "Deleted " + response.getDeleted() + " element(s)!" );

		return response.getDeleted();
	}

	/**
	 * Close the ES client properly
	 */
	public void close() {
		if( client != null ) client.close();
	}
	
}
