package com.kytsmen.spark.test

import com.kytsmen.spark.test.dto.{UserCSVHeader}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object SecondTask {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("second")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext

    val rdd = sparkContext.textFile("src/main/resources/car.txt")
    val data = rdd.map(line => line.split(",").map(e => e.trim))
    val header = new UserCSVHeader(data.first())
    val cars = data.filter(line => header(line, "id") != "id").map(el => (el(2), el(3).toInt))
    val avgNumberForTypes = cars.aggregateByKey((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(sumCount => 1.0 * sumCount._1 / sumCount._2)
      .map(element => (element._1, element._2))


    val preparedResult = avgNumberForTypes.map { case (a, b) =>
      val line = a.toString + "," + b.toString
      line
    }

    val csvHeader = sparkContext.parallelize(Array("type,avgNumber"))
    val finalResult = csvHeader.union(preparedResult).repartition(1)


    finalResult foreach println

    val fs = FileSystem.get(sparkContext.hadoopConfiguration)


    fs.delete(new Path("src/main/resources/second.csv"), true)

    finalResult.saveAsTextFile("src/main/resources/second.csv");
      //I faced with problem of adding custom csv header to

  }

}
