package com.kytsmen.spark.test.dto

import java.lang.Integer.parseInt

class Car(val id: Integer, val model: String, val `type`: String, val number: Integer, val userId: Integer, val valid: Integer) extends Serializable {
  def this(id: String, model: String, `type`: String, number: String, userId: String, valid: String) {
    this(parseInt(id), model, `type`, parseInt(number), parseInt(userId), parseInt(valid))
  }
}
