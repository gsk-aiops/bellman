package com.gsk.kg.sparqlparser

import fastparse._

object ExpressionParser {

  def parser[_: P]: P[Expression] =
    ConditionalParser.parser | BuildInFuncParser.parser | StringValParser.tripleValParser
}
