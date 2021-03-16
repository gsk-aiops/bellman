package com.gsk.kg.engine.data

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import scala.collection.immutable.Nil
import com.gsk.kg.engine.data.ChunkedList.NonEmpty
import cats.data.NonEmptyChain
import cats.implicits._
import cats.data.Chain
import cats.Traverse
import ToTree._

class ChunkedListSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with ChunkedListGenerators {

  "compact" should "converge" in {
    forAll { l: ChunkedList[Int] =>
      val a = l.compact(identity)
      val b = l.compact(identity).compact(identity)

      a shouldEqual b
    }
  }

  "fromList" should "generate a non compacted ChunkedList" in {
    forAll { l: List[Int] =>
      ChunkedList.fromList(l).mapChunks(c => c.toList should have size (1))
    }
  }

  "concat" should "concat two ChunkedLists correctly" in {
    val a = ChunkedList(1, 2, 3)
    val b = ChunkedList(4, 5, 6)
    val result = a concat b

    result shouldEqual ChunkedList(
      1, 2, 3, 4, 5, 6
    )
  }
}

trait ChunkedListGenerators {

  implicit def arb[A](implicit A: Arbitrary[A]): Arbitrary[ChunkedList[A]] =
    Arbitrary(gen)

  def gen[A](implicit A: Arbitrary[A]): Gen[ChunkedList[A]] =
    Gen.oneOf(
      Gen.const(ChunkedList.Empty[A]()),
      Gen.nonEmptyListOf(A.arbitrary).map(ChunkedList.fromList),
      Gen.nonEmptyListOf(Gen.nonEmptyListOf(A.arbitrary)).map { ls =>
        def go(l: List[List[A]]): ChunkedList[A] =
          l match {
            case Nil => ChunkedList.Empty()
            case head :: tl =>
              NonEmpty(
                NonEmptyChain.fromChainUnsafe(Chain.fromSeq(head)),
                go(tl)
              )
          }

        go(ls)
      }
    )

}
