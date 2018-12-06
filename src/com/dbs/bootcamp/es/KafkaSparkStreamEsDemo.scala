package com.dbs.bootcamp.es

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

/*
 * This class is used to read the kafka topic,
	 process the data through Structured streaming and
	 load the output into elastic search. 
 * You can apply queries on the topic data which we read from Kafka topic.
 */

object KafkaSparkStreamDemo {
  case class employee(number:Int, name:String,salary:Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .config("es.index.auto.create","true")
      .config("es.nodes","localhost")
      .config("es.port","9200")
      .config("es.http.timeout","5m")
      .config("es.scroll.size","50")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

      val schema = new StructType()
        .add($"number".string)
        .add($"name".string)
        .add($"salary".string)

      val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "KafkaEsTopic")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .option("es.index.auto.create", "true")
      .load()
      
    // Select the required columns only from the kafka streamed DF. In this case Value is required   
    val data = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]

    data.printSchema()
    println(" ----------------")
    // Need to split the data using delimiter because we get the line as string, and pass it to employee case class.
    val finalDS = data.map(_.split(",")).map(line => employee(line(0).trim.toInt, line(1).toString(),line(2).trim.toInt))
    finalDS.printSchema()

    finalDS.createOrReplaceTempView("emp")
    val sql = "select * from emp"
    val empData = spark.sql(sql);
    
//    val q = finalDS.select(sum(col("salary")).alias("Total Salary DF"))
//    q.writeStream.format("console").outputMode("complete").start()
            
    val query = empData.writeStream.format("console").outputMode("append").start()
    empData.writeStream.outputMode("append")
                          .format("org.elasticsearch.spark.sql") 
//                          .option("es.mapping.id", "mappingId")
                          .option("checkpointLocation", "c://ESCheckPoint")
                          .trigger(Trigger.ProcessingTime("5 seconds"))
                          .start("kafka/streaminges")
    spark.streams.awaitAnyTermination() // This is good for any number of queries

  }
}