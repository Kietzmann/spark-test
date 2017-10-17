package com.kytsmen.spark.test

import com.kytsmen.spark.test.dto.{User, UserCSVHeader}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object FirstTask {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("first")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    val rdd = sparkContext.textFile("src/main/resources/user.txt")

    //removing first line with format from rdd
    val data = rdd.map(line => line.split(",").map(e => e.trim))
    val header = new UserCSVHeader(data.take(1)(0))
    val rows = data.filter(line => header(line, "id") != "id")

    val users = rows.map(row => new User(row(0), row(1), row(2)))
      .filter(user => user.valid.equals(0))
      .map(user => Row(user.id, user.name))

    import sparkSession.implicits._

    //reading dataframe and filtering it
    val carsFrame = sparkSession.read.option("header", true)
      .csv("src/main/resources/car.txt")
      .select($"id", $"user_id", $"model", $"valid")

    val filteredCars = carsFrame.filter(carsFrame("valid").equalTo(0))

//creating dataframe from rdd
    val schema = new StructType()
      .add(StructField("user_id", IntegerType, true))
      .add(StructField("name", StringType, true))
    val usersDataframe = sparkSession.createDataFrame(users, schema)

    val finalResult = usersDataframe.join(filteredCars, "user_id")

    // I should write some code to extract csv file located as in hdfs (ex. part-0000*) and rename it to first.csv
    //same with parquet
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)


    fs.delete(new Path("src/main/resources/first.csv"), true)
    fs.delete(new Path("src/main/resources/first.parquet"), true)


    finalResult.write.option("header", true).csv("src/main/resources/first.csv")
    finalResult.write.option("header", true).parquet("src/main/resources/first.parquet")
  }
}
