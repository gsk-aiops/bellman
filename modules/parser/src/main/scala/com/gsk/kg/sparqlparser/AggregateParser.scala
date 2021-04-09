package com.gsk.kg.sparqlparser

import fastparse.MultiLineWhitespace._
import fastparse._

object AggregateParser {
  def count[_: P]: P[Unit]       = P("count")
  def sum[_: P]: P[Unit]         = P("sum")
  def sample[_: P]: P[Unit]      = P("sample")
  def min[_: P]: P[Unit]         = P("min")
  def max[_: P]: P[Unit]         = P("max")
  def groupConcat[_: P]: P[Unit] = P("groupConcat")
  def avg[_: P]: P[Unit]         = P("avg")

  def countParen[_: P]: P[Aggregate] =
    P("(" ~ count ~ StringValParser.variable ~ ")").map(Aggregate.COUNT(_))
  def sumParen[_: P]: P[Aggregate] =
    P("(" ~ sum ~ StringValParser.variable ~ ")").map(Aggregate.SUM(_))
  def sampleParen[_: P]: P[Aggregate] =
    P("(" ~ sample ~ StringValParser.variable ~ ")").map(Aggregate.SAMPLE(_))
  def minParen[_: P]: P[Aggregate] =
    P("(" ~ min ~ StringValParser.variable ~ ")").map(Aggregate.MIN(_))
  def avgParen[_: P]: P[Aggregate] =
    P("(" ~ avg ~ StringValParser.variable ~ ")").map(Aggregate.AVG(_))
  def maxParen[_: P]: P[Aggregate] =
    P("(" ~ max ~ StringValParser.variable ~ ")").map(Aggregate.MAX(_))
  def groupConcatParen[_: P]: P[Aggregate] =
    P(
      "(" ~
        groupConcat ~
        StringValParser.variable ~
        "\"" ~
        CharsWhile(_ != '\"').! ~
        "\"" ~
        ")"
    )
      .map(p => Aggregate.GROUP_CONCAT(p._1, p._2))

  def aggregatePatterns[_: P]: P[Aggregate] =
    P(
      countParen
        | sumParen
        | sampleParen
        | minParen
        | avgParen
        | maxParen
        | groupConcatParen
    )

  def parser[_: P]: P[Aggregate] = P(aggregatePatterns)
}
