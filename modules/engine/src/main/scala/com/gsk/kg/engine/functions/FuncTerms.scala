package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat => cc, _}
import org.apache.spark.sql.expressions.UserDefinedFunction

import com.gsk.kg.engine.functions.Literals.TypedLiteral

object FuncTerms {

  /** Returns the string representation of a column.  It only modifies the data in the column if
    * it contains an URI wrapped in angle brackets, in which case it removes it.
    * @param col
    * @return
    */
  def str(col: Column): Column =
    when(col.startsWith("<") && col.endsWith(">"), rtrim(ltrim(col, "<"), ">"))
      .otherwise(col)

  /** Returns the string representation of a column.  It only modifies the data in the column if
    * it contains an URI wrapped in angle brackets, in which case it removes it.
    * @param col
    * @return
    */
  def str(value: String): Column =
    str(lit(value))

  /** Implementation of SparQL STRDT on Spark dataframes.
    * The STRDT function constructs a literal with lexical form and type as specified by the arguments.
    *
    * Examples:
    * STRDT("123", xsd:integer) -> "123"^^<http://www.w3.org/2001/XMLSchema#integer>
    * STRDT("iiii", <http://example/romanNumeral>) -> "iiii"^^<http://example/romanNumeral>
    *
    * @param col
    * @param uri
    * @return
    */
  def strdt(col: Column, uri: String): Column =
    cc(lit("\""), col, lit("\""), lit(s"^^$uri"))

  /** The IRI function constructs an IRI by resolving the string
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
    * @param col
    * @return
    */
  def iri(col: Column): Column =
    when(
      col.startsWith("<") && col.endsWith(">"),
      col
    ).otherwise(format_string("<%s>", col))

  /** synonym for [[FuncTerms.iri]]
    *
    * @param col
    * @return
    */
  def uri(col: Column): Column = iri(col)

  /** Implementation of SparQL LANG on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-lang]]
    * @param col
    * @return
    */
  def lang(col: Column): Column =
    when(
      col.startsWith("\"") && col.contains("\"@"),
      trim(substring_index(col, "\"@", -1), "\"")
    ).otherwise(lit(""))

  /** Returns a column with 'true' or 'false' rows indicating whether a column has blank nodes
    * @param col
    * @return
    */
  def isBlank(col: Column): Column =
    when(regexp_extract(col, "^_:.*$", 0) =!= "", true)
      .otherwise(false)

  /** Implementation of SparQL ISNUMERIC on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-isNumeric]]
    * @param col
    * @return
    */
  def isNumeric(col: Column): Column = {

    /*
    This is a bit of a trick here...any valid numeric type will return true,
    even though the cast is to int
     */
    when(col.cast("int").isNotNull, lit(true))
      .when(
        TypedLiteral(col).tag.isInCollection(
          Set(
            "xsd:int",
            "xsd:integer",
            "xsd:decimal",
            "xsd:float",
            "xsd:double",
            "xsd:nonPositiveInteger",
            "xsd:negativeInteger",
            "xsd:long",
            "xsd:short",
            "xsd:nonNegativeInteger",
            "xsd:unsignedLong",
            "xsd:unsignedInt",
            "xsd:unsignedShort",
            "xsd:positiveInteger",
            "<http://www.w3.org/2001/XMLSchema#int>",
            "<http://www.w3.org/2001/XMLSchema#integer>",
            "<http://www.w3.org/2001/XMLSchema#float>",
            "<http://www.w3.org/2001/XMLSchema#decimal>",
            "<http://www.w3.org/2001/XMLSchema#double>",
            "<http://www.w3.org/2001/XMLSchema#nonPositiveInteger>",
            "<http://www.w3.org/2001/XMLSchema#negativeInteger>",
            "<http://www.w3.org/2001/XMLSchema#long>",
            "<http://www.w3.org/2001/XMLSchema#short>",
            "<http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            "<http://www.w3.org/2001/XMLSchema#unsignedLong>",
            "<http://www.w3.org/2001/XMLSchema#unsignedInt>",
            "<http://www.w3.org/2001/XMLSchema#unsignedShort>",
            "<http://www.w3.org/2001/XMLSchema#positiveInteger>"
          )
        ),
        lit(true)
      )
      .otherwise(lit(false))
  }

  /** Implementation of SparQL ISLITERAL on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-isLiteral]]
    * @param col
    * @return
    */
  def isLiteral(col: Column): Column =
    when(
      col.startsWith("\"") && col.contains("\"@"),
      lit(true)
    ).when(
      col.startsWith("\"") && col.contains("\"^^"),
      lit(true)
    ).when(
      col.startsWith("\"") && col.endsWith("\""),
      lit(true)
    ).otherwise(lit(false))

  /** Returns UUID
   * @return
   */
  def uuid: UserDefinedFunction = {
    def uuidGen: () => String = () => java.util.UUID.randomUUID().toString
    udf(uuidGen)
  }
}
