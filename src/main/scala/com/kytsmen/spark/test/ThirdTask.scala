package com.kytsmen.spark.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ColumnName, SparkSession}

object ThirdTask {
  def main(args: Array[String]): Unit = {

    val toInt = udf[Int, String](_.toInt)

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("third")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext


    val rdd = sparkSession.read.option("header", true).csv("src/main/resources/car.txt")
    val preparedDataset = rdd.withColumn("num", toInt(rdd("number")))
      .withColumn("type", rdd("type"))
    val dataset = preparedDataset.groupBy(new ColumnName("type"))
    val avgNumber = dataset.avg("num")
    val minNumber = dataset.min("num")
    val maxNumber = dataset.max("num")
    val resultFrame = avgNumber.join(minNumber, "type").join(maxNumber, "type")

    resultFrame.show()

    resultFrame.repartition(1).write.option("header", true).csv("src/main/resources/third.csv")

    //Funny thing with saving. It creates a lot of files which, I suppose, is results of each separated operation
    // After few minutes I fixed it with repartition
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)

    fs.delete(new Path("src/main/resources/third.csv"), true)

    resultFrame.write.option("header", true).csv("src/main/resources/third.csv")
  }

}
