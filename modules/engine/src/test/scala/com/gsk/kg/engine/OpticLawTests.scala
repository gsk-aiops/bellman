package com.gsk.kg.engine

import higherkindness.droste.data.Fix
import higherkindness.droste.prelude._

import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.scalacheck.DAGArbitraries
import com.gsk.kg.engine.scalacheck.DrosteImplicits

import monocle.law.discipline.IsoTests
import monocle.law.discipline.PrismTests
import org.scalacheck.Arbitrary
import org.scalactic.anyvals.PosZInt
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FlatSpecDiscipline

class OpticsLawTests
    extends AnyFlatSpec
    with Configuration
    with FlatSpecDiscipline
    with DAGArbitraries
    with DrosteImplicits {

  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(
      sizeRange = PosZInt(5),
      maxDiscardedFactor = 100
    )

  implicit val embedArbitrary: Arbitrary[Fix[DAG]] =
    embedArbitrary[DAG, Fix[DAG]]

  checkAll("basisIso", IsoTests(optics.basisIso[DAG, Fix[DAG]]))
  checkAll("_describe", PrismTests(optics._describe))
  checkAll("_ask", PrismTests(optics._ask))
  checkAll("_construct", PrismTests(optics._construct))
  checkAll("_scan", PrismTests(optics._scan))
  checkAll("_project", PrismTests(optics._project))
  checkAll("_bind", PrismTests(optics._bind))
  checkAll("_bgp", PrismTests(optics._bgp))
  checkAll("_leftjoin", PrismTests(optics._leftjoin))
  checkAll("_union", PrismTests(optics._union))
  checkAll("_filter", PrismTests(optics._filter))
  checkAll("_join", PrismTests(optics._join))
  checkAll("_offset", PrismTests(optics._offset))
  checkAll("_limit", PrismTests(optics._limit))
  checkAll("_distinct", PrismTests(optics._distinct))
  checkAll("_group", PrismTests(optics._group))
  checkAll("_noop", PrismTests(optics._noop))
}
