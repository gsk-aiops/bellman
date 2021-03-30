package com.gsk.kg.engine
package scalacheck

import cats.Show
import cats.implicits._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import java.net.URI
import java.{util => ju}

import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait DataFrameArbitraries {
  implicit val showUri: Show[URI] = Show[String].contramap(uri => s"<$uri>")

  final case class BlankNodeLabel(label: String)

  object BlankNodeLabel {
    implicit val arb: Arbitrary[BlankNodeLabel] = Arbitrary(
      CommonGenerators.nonEmptyStringGenerator.map(BlankNodeLabel.apply)
    )

    implicit val show: Show[BlankNodeLabel] =
      Show[String].contramap(str => "_:" + str.label)
  }

  sealed trait Subject
  object Subject {
    final case class SubjectUri(uri: URI)                extends Subject
    final case class SubjectBlank(blank: BlankNodeLabel) extends Subject

    implicit val arb: Arbitrary[Subject] =
      Arbitrary(
        Gen.oneOf(
          CommonGenerators.uriGen.map(SubjectUri.apply),
          BlankNodeLabel.arb.arbitrary.map(SubjectBlank.apply)
        )
      )

    implicit val show: Show[Subject] = Show.show {
      case SubjectUri(uri)     => uri.show
      case SubjectBlank(blank) => blank.show
    }
  }

  final case class Predicate(uri: URI)
  object Predicate {
    implicit val arb: Arbitrary[Predicate] = Arbitrary(
      CommonGenerators.uriGen.map(Predicate.apply)
    )
    implicit val show: Show[Predicate] = Show[URI].contramap(p => p.uri)
  }

  sealed trait Object
  object Object {
    final case class ObjectIRI(uri: URI)            extends Object
    final case class ObjectBlank(b: BlankNodeLabel) extends Object
    final case class ObjectLit(uri: Literal)        extends Object

    implicit val arb: Arbitrary[Object] =
      Arbitrary(
        Gen.oneOf(
          CommonGenerators.uriGen.map(ObjectIRI.apply),
          BlankNodeLabel.arb.arbitrary.map(ObjectBlank.apply),
          Literal.arb.arbitrary.map(ObjectLit.apply)
        )
      )

    implicit val show: Show[Object] = Show.show {
      case ObjectIRI(uri) => uri.show
      case ObjectBlank(b) => b.show
      case ObjectLit(lit) => lit.show
    }
  }

  final case class Literal(
      stringLiteral: String,
      ann: Option[LiteralAnnotation]
  )

  object Literal {

    implicit val arb: Arbitrary[Literal] = {
      val langTaggedString: Gen[Literal] =
        for {
          str <- Arbitrary.arbString.arbitrary
          locale <- Gen.oneOf(
            ju.Locale.ENGLISH,
            ju.Locale.FRENCH,
            ju.Locale.CHINESE
          )
        } yield Literal(str, Some(LiteralAnnotation.Lang(locale)))

      def schemaUri(tpe: String) = new URI(
        s"http://www.w3.org/2001/XMLSchema/$tpe"
      )

      val typeTaggedLiteral: Gen[Literal] =
        Gen.oneOf(
          Gen
            .posNum[Int]
            .map(int =>
              Literal(
                int.toString,
                Some(LiteralAnnotation.Type(schemaUri("integer")))
              )
            ),
          Gen
            .posNum[Float]
            .map(float =>
              Literal(
                float.toString,
                Some(LiteralAnnotation.Type(schemaUri("decimal")))
              )
            )
        )

      Arbitrary(
        Gen.frequency(
          1 -> langTaggedString,
          1 -> typeTaggedLiteral,
          2 -> CommonGenerators.nonEmptyStringGenerator.map(str =>
            Literal(str, None)
          )
        )
      )
    }

    implicit val show: Show[Literal] = Show.show {
      case Literal(str, None) => s""""$str""""
      case Literal(str, Some(LiteralAnnotation.Lang(locale))) =>
        s""""$str"@$locale"""
      case Literal(str, Some(LiteralAnnotation.Type(tpe))) =>
        show""""$str"^^$tpe"""
    }

  }

  sealed trait LiteralAnnotation
  object LiteralAnnotation {
    final case class Type(str: URI)        extends LiteralAnnotation
    final case class Lang(lang: ju.Locale) extends LiteralAnnotation
  }

  case class Triple(s: String, p: String, o: String)
  object Triple {

    implicit def arb(implicit
        S: Arbitrary[Subject],
        P: Arbitrary[Predicate],
        O: Arbitrary[Object]
    ): Arbitrary[Triple] =
      Arbitrary(
        for {
          s <- S.arbitrary
          p <- P.arbitrary
          o <- O.arbitrary
        } yield Triple(s.show, p.show, o.show)
      )
  }

  /** Generate valid **very random** DataFrames representing N-Triples
    * files.
    *
    * @param sc an implicit [[SQLContext]] is required to convert to [[DataFrame]]
    * @return
    */
  implicit def dataFrameGenerator(implicit
      sc: SQLContext
  ): Arbitrary[DataFrame] = {
    import sc.implicits._

    Arbitrary(
      for {
        numRows <- Gen.posNum[Int]
        triples <- Gen.listOfN(numRows, Triple.arb.arbitrary)
      } yield triples
        .map(t => (t.s, t.p, t.o))
        .toDF("s", "p", "o")
    )
  }

}

object DataFrameArbitraries extends DataFrameArbitraries
