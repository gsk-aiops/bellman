package com.gsk.kg.sparqlparser

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra

import com.gsk.kg.Graphs
import com.gsk.kg.config.Config

import scala.io.Source

trait TestUtils {

  def sparql2Algebra(fileLoc: String): String = {
    val path   = getClass.getResource(fileLoc).getPath
    val source = Source.fromFile(path)
    val sparql = source.mkString

    val query          = QueryFactory.create(sparql)
    val result: String = Algebra.compile(query).toString

    source.close
    result
  }

  def queryAlgebra(fileLoc: String)(implicit config: Config): Expr = {
    val q = readOutputFile(fileLoc)
    QueryConstruct.parseADT(q)
  }

  def query(fileLoc: String)(implicit config: Config): Query = {
    val q = readOutputFile(fileLoc)
    parse(q)._1
  }

  def readOutputFile(fileLoc: String): String = {
    val path   = getClass.getResource(fileLoc).getPath
    val source = Source.fromFile(path)

    val output = source.mkString

    source.close()
    output
  }

  def parse(
      query: String
  )(implicit config: Config): (Query, Graphs) =
    QueryConstruct.parse(query)

}
