# Kafka-spark-ElasticSearch

Pre-requisites: You system should have zookeeper, Kafka, Spark, MySQL or any database (I did use mySQL)
Before running below program,
1. Start mySQL
2. Create database - bootcamp
3. Create table - employee
4. Insert data into table - columns => number,name,salary

Below are the steps to run the Kafka-spark-ElasticSearch program.

1. Start the zookeeper
2. Satrt the kafka server
3. Start the kafka connector
    a.I have used the Kafka JDBC Connector to pump the data in Kafka topic.
4. To run the kafka connector from CMD -> 
    a.C:\kafka_2.11-1.0.0>.\bin\windows\connect-standalone.bat .\config\connect-standalone.properties .\config\connect-mysql-source-increment.properties
5.Below is my kafka connector property file. NOTE: I did use "incrementing" mode. You can ignore # lines.
    a.Example:connect-mysql-source-increment.properties 
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
 6. Once you have the data in Kafka topic, You can check with kafka consumer whether data has been produced or not.
 7. Use the sparkstructuredstreaming program to read the data from topic and process it and load data into elasticsearch.
    a.Reference class -> KafkaConnectorSparkStreamEsDemo


