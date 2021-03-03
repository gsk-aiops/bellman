package com.gsk.kg.engine

import cats.syntax.either._

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{concat => cc, _}
import com.gsk.kg.sparqlparser.BuildInFunc
import com.gsk.kg.sparqlparser.BuildInFunc._
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.StringVal.STRING

object Func {

  /**
    * Implementation of SparQL STRAFTER on Spark dataframes.
    *
    * =Examples=
    *
    * | Function call                  | Result            |
    * |:-------------------------------|:------------------|
    * | strafter("abc","b")            | "c"               |
    * | strafter("abc"@en,"ab")        | "c"@en            |
    * | strafter("abc"@en,"b"@cy)      | error             |
    * | strafter("abc"^^xsd:string,"") | "abc"^^xsd:string |
    * | strafter("abc","xyz")          | ""                |
    * | strafter("abc"@en, "z"@en)     | ""                |
    * | strafter("abc"@en, "z")        | ""                |
    * | strafter("abc"@en, ""@en)      | "abc"@en          |
    * | strafter("abc"@en, "")         | "abc"@en          |
    *
    * TODO (pepegar): Implement argument compatibility checks
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strafter]]
    * @param col
    * @param str
    * @return
    */
  def strafter(col: Column, str: String): Column =
    when(substring_index(col, str, -1) === col, lit(""))
      .otherwise(substring_index(col, str, -1))

  /**
    * The IRI function constructs an IRI by resolving the string
    * argument (see RFC 3986 and RFC 3987 or any later RFC that
    * superceeds RFC 3986 or RFC 3987). The IRI is resolved against
    * the base IRI of the query and must result in an absolute IRI.
    *
    * The URI function is a synonym for IRI.
    *
    * If the function is passed an IRI, it returns the IRI unchanged.
    *
    * Passing any RDF term other than a simple literal, xsd:string or
    * an IRI is an error.
    *
    * An implementation MAY normalize the IRI.
    *
    * =Examples=
    *
    * | Function call          | Result            |
    * |:-----------------------|:------------------|
    * | IRI("http://example/") | <http://example/> |
    * | IRI(<http://example/>) | <http://example/> |
    *
    * TODO(pepegar): We need to check if it's feasible to validate
    * that values in the columns are URI formatted.
    *
    * @param col
    * @return
    */
  def iri(col: Column): Column =
    concat(concat("<", col), ">")

  /**
    * synonym for [[Func.iri]]
    *
    * @param col
    * @return
    */
  def uri(col: Column): Column = iri(col)

  /**
    * Concatenate two [[Column]] into a new one
    *
    * @param a
    * @param b
    * @return
    */
  def concat(a: Column, b: Column): Column =
    cc(a, b)

  /**
    * Concatenate a [[String]] with a [[Column]], generating a new [[Column]]
    *
    * @param a
    * @param b
    * @return
    */
  def concat(a: String, b: Column): Column =
    concat(lit(a), b)

  /**
    * Concatenate a [[Column]] with a [[String]], generating a new [[Column]]
    *
    * @param a
    * @param b
    * @return
    */
  def concat(a: Column, b: String): Column =
    concat(a, lit(b))

}
