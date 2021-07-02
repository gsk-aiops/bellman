package com.gsk.kg.sparqlparser

import fastparse._

object ExpressionParser {

  def parser[_: P]: P[Expression] =
    ConditionalParser.parser |
      BuiltInFuncParser.parser |
      ArithmeticParser.parser |
      StringValParser.tripleValParser |
      DateTimeFuncsParser.parser
}
