package com.gsk.kg.engine

import cats.data.NonEmptyChain

sealed trait EngineError

object EngineError {
  final case class General(description: String)            extends EngineError
  final case class UnknownFunction(fn: String)             extends EngineError
  final case class UnexpectedNegative(description: String) extends EngineError
  final case class NumericTypesDoNotMatch(description: String)
      extends EngineError
  final case class FunctionError(description: String) extends EngineError
  final case class AnalyzerError(errors: NonEmptyChain[String])
      extends EngineError
}
