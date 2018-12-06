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
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

/*
 * This class is used to read the kafka topic and process the data through Structured streaming.
 * You can apply queries on the topic data which we read from Kafka topic.
 */

object KafkaConnectorSparkStreamEsDemo {
  case class employee(number:Int, name:String,salary:Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .config("es.index.auto.create","true")
      .config("es.nodes","localhost")
      .config("es.port","9200")
      .config("es.http.timeout","5m")
      .config("es.scroll.size","50")
      .appName("KafkaConnectorSparkStreamEsDemo")
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
      .option("subscribe", "mySQLIncrementData-employee")
      .option("startingOffsets", "earliest")
      .load()
      import spark.implicits._
    // Select the required columns only from the kafka streamed DF. In this case Value is required   
    val data = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]
    //We will get the value as like below. Need to parse and create employee object
    //Struct{number=18,name=kim,salary=3400}
    //Struct{number=19,name=ling,salary=3900}
    //Struct{number=20,name=sam,salary=39900}
      val finalDS = data.map(lines => { 
                              var line = lines.split(",")
                              employee(line(0).split("=")(1).toInt,
                              line(1).split("=")(1),
                              line(2).split("=")(1).dropRight(1).toInt)

    })
    finalDS.printSchema()

    finalDS.createOrReplaceTempView("emp")
    val sql = "select * from emp"
    val empDF = spark.sql(sql);
    
    //Below is example to write query using DataFrames. I am writing it to console
    //Begin
    val q = finalDS.select(avg(col("salary")))
    q.writeStream.format("console").outputMode("complete").start()
    //////End
    
    val query = empDF.writeStream
                          .format("console")
                          .trigger(Trigger.ProcessingTime("5 seconds"))
                          .outputMode("append").start()
                empDF.writeStream
                          .outputMode("append")
                          .format("org.elasticsearch.spark.sql") 
                          .option("checkpointLocation", "c://ESCheckPoint-EMP")
                          .trigger(Trigger.ProcessingTime("5 seconds"))
                          .start("es/empIncr")                          
    spark.streams.awaitAnyTermination() // This is good for any number of queries

  }
}