package com.test.sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MapReduceExample {

  def main(args: Array[String]): Unit ={

    val  conf = new SparkConf().setAppName("SparkMapReduceExample").setMaster("local[*]")
    //val session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val session = SparkSession.builder().config(conf).getOrCreate()
    //Row.from
    val data  = Seq(Row("Santosh", 1, "CTS"),Row("Savitha", 2, "Accenture"))
    val inputFields = List(StructField("name",DataTypes.StringType,false),StructField("id",DataTypes.IntegerType,false),StructField("company",DataTypes.StringType,false))
    val inputDataSetSchema = StructType(inputFields)
    val dataRDD = session.sparkContext.parallelize(data)
    val inputDataSet = session.createDataFrame(dataRDD,inputDataSetSchema)
    inputDataSet.show()

    val lines: RDD[String] = session.sparkContext.textFile("testData/someText.txt")
    val wordCount = lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((count1,count2) => count1+count2).collect
    //println(wordCount.values)
    //println(wordCount.keys)
    wordCount.foreach((entry) => println("key: "+entry._1 +" value: "+entry._2))




  }





}
