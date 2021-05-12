package com.gsk.kg.engine

import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.lang.CollectorStreamTriples

import org.apache.spark.sql.SQLContext

package object compiler {

  def readNTtoDF(path: String)(implicit sc: SQLContext) = {

    import scala.collection.JavaConverters._
    import sc.implicits._

    val filename                            = s"modules/engine/src/test/resources/$path"
    val inputStream: CollectorStreamTriples = new CollectorStreamTriples()
    RDFParser.source(filename).parse(inputStream)

    inputStream
      .getCollected()
      .asScala
      .toList
      .map(triple =>
        (
          triple.getSubject().toString(),
          triple.getPredicate().toString(),
          triple.getObject().toString()
        )
      )
      .toDF("s", "p", "o")
  }

}
