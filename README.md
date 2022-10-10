ElasticSearch CRUD Operations Practical Task: 3 points

1. Install ElasticSearch.

2. Create JSON object that represents information about event.

Event object should include following fields: * title * event type (workshop / tech-talk) * date and time * place * description * list of sub-topics

3. Store, get, full/partial update, delete document from the elastic using Document REST API. Use some of Rest clients such as Postman Chrome plugin.

4. Populate documents to ElasticSearch which represent different events. Perform search queries using Search API to select:

* all events; * workshop events only; * events with title XXX; * events which were held after date xx-xx-xx and with title XXX; * calculate amount of events for each event type using ElasticSearch aggregations. (Note: use field_name.keyword)

5. Implement ElasticJavaApiDataProvider which uses Java API client for retrieval of events info.

6. Implement ElasticJavaRestApiDataProvider which uses REST Service client for retrieval of events info.

7. Do the following from java code (using both clients):

* create new index; * apply new mapping to the previously created index; * create Event class and store JSON representation of user instances into ElasticSearch; * perform search queries to select: - all events; - workshop events only; - events with title XXX; - events which were held after date xx-xx-xx and with name XXX; Note: Serialize result documents back to Event Java instances.

8. Delete all events with title NNN (BULK API) in one request.

Filebeat, Logstash, Kibana Practical Task: 1 points

1. Install ElasticSearch 6.5.x+.

2. Install Logstash 6.5.x+.

3. Install Kibana 6.5.x+.

4. Configure event-service Java application to use log4j and log messages using to the file on file system.

5. Configure Filebeat to read log file and send log messages into the Logstash.

6. Configure Logstash to read logs from the filebeat (beats input plugin) and send logs to Elasticsearch (elasticsearch logstash output plugin).

7. Look at result data in the ElasticSearch using Kibana. Task result: application, logstash and filebeat configuration files.

Logstash Filters Practical Task: 1 point

1. Configure event-service Java application to use log4j and log messages using to the file on file system. Log messages should have the following pattern:

${MESSAGE_ID} [${UUID} key1="value1" key2="value2" key3="value3" keyN="ValueN"] ${PROCESS_ID} ${COMPONENT_NAME}: ${MESSAGE} Example: 00-00-00000 [c133ee9a7bfa11e6ae2256b6b6499611 app_name="application-name" app_version="1.0.0-SNAPSHOT" hostname="localhost"] 69427d6c966046c58804d7f4128f7505 DataBase: message here

2. Read all the logs from the file and send them to logstash.

3. Using Logstash filters parse log messages and extract all the fields specified in the message pattern (result document in the ElasticSearch should contains all of them in the root of log document).

4. Add ${AUTHOR} field to every log messages using logstash filters.

5. If ${MESSAGE} contains keyword "error" - add tag [failed] to this document.

NOTE: Use Kibana to make sure logs are properly parsed and contains all required fields. Task result: application, filebeat and logstash configuration file.

