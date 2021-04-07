package com.gsk.kg.sparqlparser

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra

import scala.io.Source

object TestUtils {

  def sparql2Algebra(fileLoc: String): String = {
    val path   = getClass.getResource(fileLoc).getPath
    val sparql = Source.fromFile(path).mkString

    val query = QueryFactory.create(sparql)
    Algebra.compile(query).toString
  }

  def queryAlgebra(fileLoc: String): Expr = {
    val q = readOutputFile(fileLoc)
    QueryConstruct.parseADT(q)
  }

  def query(fileLoc: String): Query = {
    val q = readOutputFile(fileLoc)
    QueryConstruct.parse(q)._1
  }

  def readOutputFile(fileLoc: String): String = {
    val path = getClass.getResource(fileLoc).getPath
    Source.fromFile(path).mkString
  }

}
