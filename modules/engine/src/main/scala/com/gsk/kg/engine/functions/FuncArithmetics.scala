package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column

object FuncArithmetics {

  def add(l: Column, r: Column): Column =
    l + r

  def subtract(l: Column, r: Column): Column =
    l - r

  def multiply(l: Column, r: Column): Column =
    l * r

  def divide(l: Column, r: Column): Column =
    l / r
}
