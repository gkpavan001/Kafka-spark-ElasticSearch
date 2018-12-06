package com.dbs.bootcamp.es
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._


object SampleEs {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Elastic")
				.setMaster("local[3]")
				.set("es.index.auto.create","true")
				.set("es.nodes","127.0.0.1")
				.set("es.port","9200")
				.set("es.http.timeout","5m")
				.set("es.scroll.size","50")
				.set("es.nodes.discovery","false")
				val sc = new SparkContext(conf)
		    sc.setLogLevel("ERROR")
				val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
				val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
				sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
	}
}