package com.kytsmen.spark.test.dto

import java.lang.Integer.parseInt

class User(val id: Integer, val name:String, val valid: Integer) {
  def this(id: String, name: String, valid: String){
    this(parseInt(id), name, parseInt(valid))
  }
}
