package com.gsk.kg

import cats.syntax.either._

import com.gsk.kg.sparqlparser.EngineError.ParsingError

import fastparse.Parsed
import fastparse.Parsed.Failure
import fastparse.Parsed.Success

package object sparqlparser {

  /** the type for operations that may fail
    */
  type Result[A] = Either[EngineError, A]
  val Result = Either

  implicit class RichParsed[T](parsed: Parsed[T]) {
    def toParserResult: Result[T] = parsed match {
      case Success(value, _) => value.asRight
      case Failure(label, index, extra) =>
        ParsingError(s"$label at position $index, ${extra.input}").asLeft
    }
  }
}
