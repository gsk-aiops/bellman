package com.gsk.kg.engine

sealed trait EngineError

object EngineError {
  case class General(description: String) extends EngineError
  case class UnknownFunction(fn: String) extends EngineError
  case class UnexpectedNegative(description: String) extends EngineError
  case class NumericTypesDoNotMatch(description: String) extends EngineError
  case class FunctionError(description: String) extends EngineError
}
