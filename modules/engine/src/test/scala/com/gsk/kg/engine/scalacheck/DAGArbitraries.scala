package com.gsk.kg.engine
package scalacheck

import cats._
import cats.implicits._
import cats.arrow.FunctionK
import cats.data.NonEmptyList

import higherkindness.droste._
import higherkindness.droste.syntax.compose._

import org.scalacheck._
import org.scalacheck.cats.implicits._
import org.scalacheck.Gen

import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.Expression

trait DAGArbitraries
    extends CommonGenerators
    with ExpressionArbitraries
    with ChunkedListArbitraries {

  import DAG._

  val variableGenerator: Gen[VARIABLE] = nonEmptyStringGenerator.map(VARIABLE(_))

  val tripleGenerator: Gen[Expr.Triple] =
    for {
      s <- stringValGenerator
      p <- stringValGenerator
      o <- stringValGenerator
    } yield Expr.Triple(s, p, o)

  implicit val tripleArbitrary: Arbitrary[Expr.Triple] = Arbitrary(tripleGenerator)

  val bgpGenerator: Gen[Expr.BGP] = Gen.nonEmptyContainerOf[Seq, Expr.Triple](tripleGenerator)
    .map(Expr.BGP(_))

  val expressionNelGenerator: Gen[NonEmptyList[Expression]] =
    Gen.nonEmptyListOf(expressionGenerator).map(l => NonEmptyList.fromListUnsafe(l))

  implicit def dagArbitrary: Delay[Arbitrary, DAG] =
    λ[Arbitrary ~> (Arbitrary ∘ DAG)#λ] { arbA =>
      Arbitrary(
        Gen.oneOf(
          (Gen.listOf(variableGenerator), Gen.lzy(arbA.arbitrary)).mapN(describe(_, _)),
          Gen.lzy(arbA.arbitrary).map(ask(_)),
          (bgpGenerator, Gen.lzy(arbA.arbitrary)).mapN(construct(_, _)),
          (Gen.alphaStr, Gen.lzy(arbA.arbitrary)).mapN(scan(_, _)),
          (Gen.nonEmptyListOf(variableGenerator), Gen.lzy(arbA.arbitrary)).mapN(project(_, _)),
          (variableGenerator, expressionGenerator, Gen.lzy(arbA.arbitrary)).mapN(bind(_, _, _)),
          (Gen.lzy(arbA.arbitrary), Gen.lzy(arbA.arbitrary), Gen.listOf(expressionGenerator)).mapN(leftJoin(_, _, _)),
          (Gen.lzy(arbA.arbitrary), Gen.lzy(arbA.arbitrary)).mapN(union(_, _)),
          (expressionNelGenerator, Gen.lzy(arbA.arbitrary)).mapN(filter(_, _)),
          (Gen.lzy(arbA.arbitrary), Gen.lzy(arbA.arbitrary)).mapN(join(_, _)),
          (Gen.long, Gen.lzy(arbA.arbitrary)).mapN(offset(_, _)),
          (Gen.long, Gen.lzy(arbA.arbitrary)).mapN(limit(_, _)),
          Gen.lzy(arbA.arbitrary).map(distinct(_)),
          chunkedListGenerator[Expr.Triple].map(bgp[A0$](_))
          )
      )
    }
}

object DAGArbitraries extends DAGArbitraries
