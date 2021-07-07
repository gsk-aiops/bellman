package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

import fastparse.MultiLineWhitespace._
import fastparse._

object ExprParser {
  /*
  Graph patterns
   */
  def sequence[_: P]: P[Unit]    = P("sequence")
  def bgp[_: P]: P[Unit]         = P("bgp")
  def path[_: P]: P[Unit]        = P("path")
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
  def reduced[_: P]: P[Unit]     = P("reduced")
  def group[_: P]: P[Unit]       = P("group")
  def order[_: P]: P[Unit]       = P("order")
  def table[_: P]: P[Unit]       = P("table")
  def row[_: P]: P[Unit]         = P("row")
  def vars[_: P]: P[Unit]        = P("vars")
  def exists[_: P]: P[Unit]      = P("exists")
  def notExists[_: P]: P[Unit]   = P("notexists")
  def not[_: P]: P[Unit]         = P("!")
  def minus[_: P]: P[Unit]       = P("minus")

  def opNull[_: P]: P[OpNil]      = P("(null)").map(_ => OpNil())
  def tableUnit[_: P]: P[TabUnit] = P("(table unit)").map(_ => TabUnit())

  def bgpOrPathParen[_: P]: P[Expr] = bgpParen | pathQuadParen

  def sequenceParen[_: P]: P[Sequence] = P(
    "(" ~ sequence ~ bgpOrPathParen.rep(1) ~ ")"
  ).map(s => Sequence(s.toList))

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
    P("(" ~ distinct ~ graphPattern).map(Distinct)

  def reducedParen[_: P]: P[Reduced] =
    P("(" ~ reduced ~ graphPattern).map(Reduced)

  def triple[_: P]: P[Quad] =
    P(
      "(triple" ~
        StringValParser.tripleValParser ~
        StringValParser.tripleValParser ~
        StringValParser.tripleValParser ~ ")"
    ).map(t => Quad(t._1, t._2, t._3, GRAPH_VARIABLE :: Nil))

  def bgpParen[_: P]: P[BGP] = P("(" ~ bgp ~ triple.rep(1) ~ ")").map(BGP)

  def pathQuadParen[_: P]: P[Path] = P(
    "(" ~ path ~ StringValParser.tripleValParser ~ PropertyPathParser.parser ~ StringValParser.tripleValParser ~ ")"
  ).map(p => Path(p._1, p._2, p._3, GRAPH_VARIABLE :: Nil))

  def exprFunc[_: P]: P[Expression] =
    ConditionalParser.parser |
      BuiltInFuncParser.parser |
      AggregateParser.parser |
      ArithmeticParser.parser |
      DateTimeFuncsParser.parser

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

  def assignment[_: P]: P[Seq[(StringVal.VARIABLE, Expression)]] = P(
    "(" ~ ("(" ~ StringValParser.variable ~ exprFunc ~ ")").rep ~ ")"
  )

  def groupParen[_: P]: P[Group] = P(
    "(" ~ group ~ "(" ~ (StringValParser.variable).rep(
      0
    ) ~ ")" ~ assignment.? ~ graphPattern ~ ")"
  ).map(p =>
    Group(
      p._1,
      p._2.fold(Seq.empty[(StringVal.VARIABLE, Expression)])(identity),
      p._3
    )
  )

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
    "(" ~ order ~ "(" ~ OrderConditionParser.parser.rep(
      1
    ) ~ ")" ~ graphPattern ~ ")"
  ).map { p =>
    Order(p._1, p._2)
  }

  def tupleParen[_: P]: P[(VARIABLE, StringVal)] = P(
    "[" ~ StringValParser.variable ~ StringValParser.tripleValParser ~ "]"
  )

  def rowParen[_: P]: P[Row] = P(
    "(" ~ row ~ tupleParen.rep(0) ~ ")"
  ).map(Row)

  def tableParen[_: P]: P[Table] = P(
    "(" ~ table ~ "(" ~ vars ~ StringValParser.variable.rep(0) ~ ")" ~ rowParen
      .rep(
        0
      ) ~ ")"
  ).map { case (vs, rs) => Table(vs, rs) }

  def existsParen[_: P]: P[Exists] = {
    P("(" ~ filter ~ "(" ~ exists ~ graphPattern ~ ")" ~ graphPattern ~ ")")
      .map { p =>
        Exists(not = false, p._1, p._2)
      } |
      P(
        "(" ~ filter ~ "(" ~ not ~ "(" ~ exists ~ graphPattern ~ ")" ~ ")" ~ graphPattern ~ ")"
      ).map { p =>
        Exists(not = true, p._1, p._2)
      } |
      P(
        "(" ~ filter ~ "(" ~ notExists ~ graphPattern ~ ")" ~ graphPattern
      ).map { p =>
        Exists(not = true, p._1, p._2)
      }
  }

  def minusParen[_: P]: P[Minus] = P(
    "(" ~ minus ~ graphPattern ~ graphPattern ~ ")"
  ).map { p =>
    Minus(p._1, p._2)
  }

  def graphPattern[_: P]: P[Expr] =
    P(
      selectParen
        | sequenceParen
        | pathQuadParen
        | offsetLimitParen
        | distinctParen
        | reducedParen
        | leftJoinParen
        | filteredLeftJoinParen
        | joinParen
        | graphParen
        | bgpParen
        | unionParen
        | minusParen
        | extendParen
        | filterSingleParen
        | filterListParen
        | groupParen
        | orderParen
        | tableParen
        | existsParen
        | opNull
        | tableUnit
    )

  def parser[_: P]: P[Expr] = P(graphPattern)
}
