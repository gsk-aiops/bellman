package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE

import fastparse.MultiLineWhitespace._
import fastparse._

object ExprParser {
  /*
  Graph patterns
   */
  def bgp[_: P]: P[Unit]         = P("bgp")
  def leftJoin[_: P]: P[Unit]    = P("leftjoin")
  def union[_: P]: P[Unit]       = P("union")
  def extend[_: P]: P[Unit]      = P("extend")
  def filter[_: P]: P[Unit]      = P("filter")
  def exprList[_: P]: P[Unit]    = P("exprlist")
  def join[_: P]: P[Unit]        = P("join")
  def graph[_: P]: P[Unit]       = P("graph")
  def select[_: P]: P[Unit]      = P("project")
  def offsetLimit[_: P]: P[Unit] = P("slice")
  def distinct[_: P]: P[Unit]    = P("distinct")
  def group[_: P]: P[Unit]       = P("group")
  def order[_: P]: P[Unit]       = P("order")

  def opNull[_: P]: P[OpNil]      = P("(null)").map(_ => OpNil())
  def tableUnit[_: P]: P[TabUnit] = P("(table unit)").map(_ => TabUnit())

  def selectParen[_: P]: P[Project] = P(
    "(" ~ select ~ "(" ~ (StringValParser.variable).rep(
      1
    ) ~ ")" ~ graphPattern ~ ")"
  )
    .map(p => Project(p._1, p._2))

  def offsetLimitParen[_: P]: P[OffsetLimit] = P(
    "(" ~ offsetLimit ~
      StringValParser.optionLong ~
      StringValParser.optionLong ~
      graphPattern ~ ")"
  ).map { ops =>
    OffsetLimit(ops._1, ops._2, ops._3)
  }

  def distinctParen[_: P]: P[Distinct] =
    P("(" ~ distinct ~ graphPattern).map(Distinct(_))

  def triple[_: P]: P[Quad] =
    P(
      "(triple" ~
        StringValParser.tripleValParser ~
        StringValParser.tripleValParser ~
        StringValParser.tripleValParser ~ ")"
    ).map(t => Quad(t._1, t._2, t._3, GRAPH_VARIABLE :: Nil))

  def bgpParen[_: P]: P[BGP] = P("(" ~ bgp ~ triple.rep(1) ~ ")").map(BGP(_))

  def exprFunc[_: P]: P[Expression] =
    ConditionalParser.parser | BuiltInFuncParser.parser | AggregateParser.parser

  def filterExprList[_: P]: P[Seq[Expression]] =
    P("(" ~ exprList ~ exprFunc.rep(2) ~ ")")

  def exprFuncList[_: P]: P[Seq[Expression]] =
    (filterExprList | exprFunc).flatMap {

      case e: Seq[_] =>
        ParsingRun.current.freshSuccess(e.asInstanceOf[Seq[Expression]])
      case e: Expression => ParsingRun.current.freshSuccess(Seq(e))
      case _ =>
        ParsingRun.current.freshFailure()
    }

  def assignment[_: P]: P[(StringVal.VARIABLE, Expression)] = P(
    "((" ~ StringValParser.variable ~ exprFunc ~ "))"
  )

  def groupParen[_: P]: P[Group] = P(
    "(" ~ group ~ "(" ~ (StringValParser.variable).rep(
      1
    ) ~ ")" ~ assignment.repX(max = 1) ~ graphPattern ~ ")"
  ).map(p => Group(p._1, p._2.headOption, p._3))

  def filterListParen[_: P]: P[Filter] =
    P("(" ~ filter ~ filterExprList ~ graphPattern ~ ")").map { p =>
      Filter(p._1, p._2)
    }

  def filterSingleParen[_: P]: P[Filter] =
    P("(" ~ filter ~ exprFunc ~ graphPattern ~ ")").map { p =>
      Filter(List(p._1), p._2)
    }

  def leftJoinParen[_: P]: P[LeftJoin] =
    P("(" ~ leftJoin ~ graphPattern ~ graphPattern ~ ")").map { lj =>
      LeftJoin(lj._1, lj._2)
    }

  def filteredLeftJoinParen[_: P]: P[FilteredLeftJoin] =
    P("(" ~ leftJoin ~ graphPattern ~ graphPattern ~ exprFuncList ~ ")").map {
      lj => FilteredLeftJoin(lj._1, lj._2, lj._3)
    }

  def unionParen[_: P]: P[Union] =
    P("(" ~ union ~ graphPattern ~ graphPattern ~ ")").map { u =>
      Union(u._1, u._2)
    }

  def extendParen[_: P]: P[Extend] = P(
    "(" ~
      extend ~ "((" ~ (StringValParser.variable) ~
      (StringValParser.tripleValParser | exprFunc) ~ "))" ~
      graphPattern ~ ")"
  ).map { ext =>
    Extend(ext._1, ext._2, ext._3)
  }

  def joinParen[_: P]: P[Join] =
    P("(" ~ join ~ graphPattern ~ graphPattern ~ ")").map { p =>
      Join(p._1, p._2)
    }

  def graphParen[_: P]: P[Graph] = P(
    "(" ~ graph ~ (StringValParser.urival | StringValParser.variable) ~ graphPattern ~ ")"
  ).map { p =>
    Graph(p._1, p._2)
  }

  def orderParen[_: P]: P[Order] = P(
    "(" ~ order ~ "(" ~ StringValParser.variable ~ ")" ~ graphPattern ~ ")"
  ).map { p =>
    Order(p._1, p._2)
  }

  def graphPattern[_: P]: P[Expr] =
    P(
      selectParen
        | offsetLimitParen
        | distinctParen
        | leftJoinParen
        | filteredLeftJoinParen
        | joinParen
        | graphParen
        | bgpParen
        | unionParen
        | extendParen
        | filterSingleParen
        | filterListParen
        | groupParen
        | orderParen
        | opNull
        | tableUnit
    )

  def parser[_: P]: P[Expr] = P(graphPattern)
}
