package com.gsk.kg.engine
package scalacheck

import cats._
import cats.data._
import higherkindness.droste._
import higherkindness.droste.syntax.compose._
import org.scalacheck.Arbitrary
import cats.arrow.FunctionK
import org.scalacheck.Gen
import com.gsk.kg.engine.data.ChunkedList
import cats.data.Chain

trait ChunkedListArbitraries {

  implicit def arb[A](implicit A: Arbitrary[A]): Arbitrary[ChunkedList[A]] =
    Arbitrary(chunkedListGenerator)

  def chunkedListGenerator[A](implicit A: Arbitrary[A]): Gen[ChunkedList[A]] =
    Gen.oneOf(
      Gen.const(ChunkedList.Empty[A]()),
      Gen.nonEmptyListOf(A.arbitrary).map(ChunkedList.fromList),
      Gen.nonEmptyListOf(Gen.nonEmptyListOf(A.arbitrary)).map { ls =>
        def go(l: List[List[A]]): ChunkedList[A] =
          l match {
            case Nil => ChunkedList.Empty()
            case head :: tl =>
              ChunkedList.NonEmpty(
                NonEmptyChain.fromChainUnsafe(Chain.fromSeq(head)),
                go(tl)
              )
          }

        go(ls)
      }
    )

}

object ChunkedListArbitraries extends ChunkedListArbitraries
