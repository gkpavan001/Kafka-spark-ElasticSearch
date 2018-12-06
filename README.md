# Kafka-spark-ElasticSearch

I have used the Kafka JDBC Connector to pump the data in Kafka topic. I have used the "incrementing" as MODE.

To run the kafka connector from CMD -> 
C:\kafka_2.11-1.0.0>.\bin\windows\connect-standalone.bat .\config\connect-standalone.properties .\config\connect-mysql-source-increment.properties


Example:connect-mysql-source-increment.properties
////
name=test-source-mysql-jdbc-incr
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
tasks.max=1
connection.url=jdbc:mysql://localhost:3306/bootcamp?user=root&password=1234
#query=SELECT * FROM EMPLOYEE
table.types=TABLE
table.blacklist=
table.whitelist=employee
mode=incrementing
incrementing.column.name=number
topic.prefix=mySQLIncrementData-
#Below property is used to have a time gap to hit the database and fetch new data -> 20 seconds
poll.interval.ms=20000
/////

Once you have the data in Kafka topic, use the sparkstructuredstreaming to read the data from topic and process it and load data into
elasticsearch.
Reference class -> KafkaConnectorSparkStreamEsDemo


