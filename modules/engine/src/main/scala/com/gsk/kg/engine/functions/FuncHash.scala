package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.functions.Literals.extractStringLiteral

object FuncHash {

  /** Implementation of SparQL MD5 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-md5]]
    * @param col
    * @return
    */
  def md5(col: Column): Column =
    functions.md5(extractStringLiteral(col))

  /** Implementation of SparQL MD5 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-md5]]
    * @param str
    * @return
    */
  def md5(str: String): Column =
    functions.md5(lit(extractStringLiteral(str)))

  /** Implementation of SparQL SHA1 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha1]]
    * @param col
    * @return
    */
  def sha1(col: Column): Column =
    functions.sha1(extractStringLiteral(col))

  /** Implementation of SparQL SHA1 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha1]]
    * @param str
    * @return
    */
  def sha1(str: String): Column =
    functions.sha1(lit(extractStringLiteral(str)))

  /** Implementation of SparQL SHA256 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha256]]
    * @param col
    * @return
    */
  def sha256(col: Column): Column = {
    val numBits = 256
    sha2(extractStringLiteral(col), numBits)
  }

  /** Implementation of SparQL SHA256 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha256]]
    * @param str
    * @return
    */
  def sha256(str: String): Column = {
    val numBits = 256
    sha2(lit(extractStringLiteral(str)), numBits)
  }

  /** Implementation of SparQL SHA384 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha384]]
    * @param col
    * @return
    */
  def sha384(col: Column): Column = {
    val numBits = 384
    sha2(extractStringLiteral(col), numBits)
  }

  /** Implementation of SparQL SHA384 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha384]]
    * @param str
    * @return
    */
  def sha384(str: String): Column = {
    val numBits = 384
    sha2(lit(extractStringLiteral(str)), numBits)
  }

  /** Implementation of SparQL SHA512 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha512]]
    * @param col
    * @return
    */
  def sha512(col: Column): Column = {
    val numBits = 512
    sha2(extractStringLiteral(col), numBits)
  }

  /** Implementation of SparQL SHA512 on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-sha512]]
    * @param str
    * @return
    */
  def sha512(str: String): Column = {
    val numBits = 512
    sha2(lit(extractStringLiteral(str)), numBits)
  }
}
