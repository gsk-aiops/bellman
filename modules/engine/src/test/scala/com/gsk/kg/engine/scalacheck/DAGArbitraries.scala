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

  val variableGenerator: Gen[VARIABLE] =
    nonEmptyStringGenerator.map(VARIABLE(_))

  val tripleGenerator: Gen[Expr.Triple] =
    (
      stringValGenerator,
      stringValGenerator,
      stringValGenerator
    ).mapN(Expr.Triple(_, _, _))

  implicit val tripleArbitrary: Arbitrary[Expr.Triple] = Arbitrary(
    tripleGenerator
  )

  val exprBgpGenerator: Gen[Expr.BGP] = smallNonEmptyListOf(tripleGenerator).map(Expr.BGP(_))

  val expressionNelGenerator: Gen[NonEmptyList[Expression]] =
    smallNonEmptyListOf(expressionGenerator)
      .map(l => NonEmptyList.fromListUnsafe(l))

  def describeGenerator[A](implicit A: Arbitrary[A]): Gen[Describe[A]] =
    (smallListOf(variableGenerator), Gen.lzy(A.arbitrary)).mapN(Describe(_, _))
  def askGenerator[A](implicit A: Arbitrary[A]): Gen[Ask[A]] =
    Gen.lzy(A.arbitrary).map(Ask(_))
  def constructGenerator[A](implicit A: Arbitrary[A]): Gen[Construct[A]] =
    (exprBgpGenerator, Gen.lzy(A.arbitrary)).mapN(Construct(_, _))
  def scanGenerator[A](implicit A: Arbitrary[A]): Gen[Scan[A]] =
    (Gen.alphaStr, Gen.lzy(A.arbitrary)).mapN(Scan(_, _))
  def projectGenerator[A](implicit A: Arbitrary[A]): Gen[Project[A]] =
    (smallNonEmptyListOf(variableGenerator), Gen.lzy(A.arbitrary))
      .mapN(Project(_, _))
  def bindGenerator[A](implicit A: Arbitrary[A]): Gen[Bind[A]] =
    (variableGenerator, expressionGenerator, Gen.lzy(A.arbitrary))
      .mapN(Bind(_, _, _))
  def leftJoinGenerator[A](implicit A: Arbitrary[A]): Gen[LeftJoin[A]] =
    (
      Gen.lzy(A.arbitrary),
      Gen.lzy(A.arbitrary),
      smallListOf(expressionGenerator)
    ).mapN(LeftJoin(_, _, _))
  def unionGenerator[A](implicit A: Arbitrary[A]): Gen[Union[A]] =
    (Gen.lzy(A.arbitrary), Gen.lzy(A.arbitrary)).mapN(Union(_, _))
  def filterGenerator[A](implicit A: Arbitrary[A]): Gen[Filter[A]] =
    (expressionNelGenerator, Gen.lzy(A.arbitrary)).mapN(Filter(_, _))
  def joinGenerator[A](implicit A: Arbitrary[A]): Gen[Join[A]] =
    (Gen.lzy(A.arbitrary), Gen.lzy(A.arbitrary)).mapN(Join(_, _))
  def offsetGenerator[A](implicit A: Arbitrary[A]): Gen[Offset[A]] =
    (Gen.long, Gen.lzy(A.arbitrary)).mapN(Offset(_, _))
  def limitGenerator[A](implicit A: Arbitrary[A]): Gen[Limit[A]] =
    (Gen.long, Gen.lzy(A.arbitrary)).mapN(Limit(_, _))
  def distinctGenerator[A](implicit A: Arbitrary[A]): Gen[Distinct[A]] =
    Gen.lzy(A.arbitrary).map(Distinct(_))
  def bgpGenerator[A](implicit A: Arbitrary[A]): Gen[BGP[A]] =
    chunkedListGenerator[Expr.Triple].map(BGP(_))
  def noopGenerator[A]: Gen[Noop[A]] =
    Arbitrary.arbString.arbitrary.map(Noop[A](_))

  implicit def dagArbitrary: Delay[Arbitrary, DAG] =
    λ[Arbitrary ~> (Arbitrary ∘ DAG)#λ] { arbA =>
      Arbitrary(
        Gen.oneOf(
          describeGenerator(arbA),
          askGenerator(arbA),
          constructGenerator(arbA),
          scanGenerator(arbA),
          projectGenerator(arbA),
          bindGenerator(arbA),
          leftJoinGenerator(arbA),
          unionGenerator(arbA),
          filterGenerator(arbA),
          joinGenerator(arbA),
          offsetGenerator(arbA),
          limitGenerator(arbA),
          distinctGenerator(arbA),
          bgpGenerator(arbA)
        )
      )
    }

  implicit def arbitraryDescribe[T: Arbitrary]: Arbitrary[DAG.Describe[T]] = Arbitrary(describeGenerator[T])
  implicit def arbitraryAsk[T: Arbitrary]: Arbitrary[DAG.Ask[T]] = Arbitrary(askGenerator[T])
  implicit def arbitraryConstruct[T: Arbitrary]: Arbitrary[DAG.Construct[T]] = Arbitrary(constructGenerator[T])
  implicit def arbitraryScan[T: Arbitrary]: Arbitrary[DAG.Scan[T]] = Arbitrary(scanGenerator[T])
  implicit def arbitraryProject[T: Arbitrary]: Arbitrary[DAG.Project[T]] = Arbitrary(projectGenerator[T])
  implicit def arbitraryBind[T: Arbitrary]: Arbitrary[DAG.Bind[T]] = Arbitrary(bindGenerator[T])
  implicit def arbitraryLeftJoin[T: Arbitrary]: Arbitrary[DAG.LeftJoin[T]] = Arbitrary(leftJoinGenerator[T])
  implicit def arbitraryUnion[T: Arbitrary]: Arbitrary[DAG.Union[T]] = Arbitrary(unionGenerator[T])
  implicit def arbitraryFilter[T: Arbitrary]: Arbitrary[DAG.Filter[T]] = Arbitrary(filterGenerator[T])
  implicit def arbitraryJoin[T: Arbitrary]: Arbitrary[DAG.Join[T]] = Arbitrary(joinGenerator[T])
  implicit def arbitraryOffset[T: Arbitrary]: Arbitrary[DAG.Offset[T]] = Arbitrary(offsetGenerator[T])
  implicit def arbitraryLimit[T: Arbitrary]: Arbitrary[DAG.Limit[T]] = Arbitrary(limitGenerator[T])
  implicit def arbitraryDistinct[T: Arbitrary]: Arbitrary[DAG.Distinct[T]] = Arbitrary(distinctGenerator[T])
  implicit def arbitraryBgp[T: Arbitrary]: Arbitrary[DAG.BGP[T]] = Arbitrary(bgpGenerator[T])
  implicit def arbitraryNoop[T: Arbitrary]: Arbitrary[DAG.Noop[T]] = Arbitrary(noopGenerator[T])

  implicit def cogenDag[T]: Cogen[DAG[T]] =
    Cogen.cogenString.contramap(_.toString)

  implicit def cogenDescribe[A]: Cogen[Describe[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenAsk[A]: Cogen[Ask[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenConstruct[A]: Cogen[Construct[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenScan[A]: Cogen[Scan[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenProject[A]: Cogen[Project[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenBind[A]: Cogen[Bind[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenBGP[A]: Cogen[BGP[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenLeftJoin[A]: Cogen[LeftJoin[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenUnion[A]: Cogen[Union[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenFilter[A]: Cogen[Filter[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenJoin[A]: Cogen[Join[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenOffset[A]: Cogen[Offset[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenLimit[A]: Cogen[Limit[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenDistinct[A]: Cogen[Distinct[A]] = Cogen.cogenString.contramap(_.toString)
  implicit def cogenNoop[A]: Cogen[Noop[A]] = Cogen.cogenString.contramap(_.toString)

}

object DAGArbitraries extends DAGArbitraries
