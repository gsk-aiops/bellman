package com.gsk.kg.engine
package scalacheck

import cats.data.Chain
import cats.data._

import com.gsk.kg.engine.data.ChunkedList

import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait ChunkedListArbitraries extends CommonGenerators {

  implicit def arb[A](implicit A: Arbitrary[A]): Arbitrary[ChunkedList[A]] =
    Arbitrary(chunkedListGenerator)

  def chunkedListGenerator[A](implicit A: Arbitrary[A]): Gen[ChunkedList[A]] =
    Gen.oneOf(
      smallListOf(A.arbitrary).map(ChunkedList.fromList),
      smallNonEmptyListOf(smallNonEmptyListOf(A.arbitrary)).map { ls =>
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
