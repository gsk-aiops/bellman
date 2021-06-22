package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column

import com.gsk.kg.engine.functions.Literals._

object FuncArithmetics {

  def add(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ + _)

  def subtract(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ - _)

  def multiply(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ * _)

  def divide(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ / _)
}
