package com.kytsmen.spark.test.dto

class UserCSVHeader(header: Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap

  def apply(array: Array[String], key: String): String = array(index(key))
}
