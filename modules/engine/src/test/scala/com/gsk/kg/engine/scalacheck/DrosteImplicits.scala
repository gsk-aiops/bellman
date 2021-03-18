package com.gsk.kg.engine
package scalacheck

import higherkindness.droste._
import higherkindness.droste.syntax.all._
import org.scalacheck.Arbitrary
import cats.Functor
import org.scalacheck.Gen

trait DrosteImplicits {

  implicit def delayArbitrary[F[_], A](
    implicit
    A: Arbitrary[A],
    F: Delay[Arbitrary, F]
  ): Arbitrary[F[A]] =
    F(A)

  implicit def embedArbitrary[F[_]: Functor, T]
    (implicit T: higherkindness.droste.Embed[F, T], fArb: Delay[Arbitrary, F])
      : Arbitrary[T] =
    Arbitrary(Gen.sized(size =>
      fArb(Arbitrary(
        if (size <= 0)
          Gen.fail[T]
        else
          Gen.resize(size - 1, embedArbitrary[F, T].arbitrary))).arbitrary map (_.embed)))

}

object DrosteImplicits extends DrosteImplicits
