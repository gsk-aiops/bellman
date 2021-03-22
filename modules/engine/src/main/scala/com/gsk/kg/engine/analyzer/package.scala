package com.gsk.kg.engine

import cats.data.ValidatedNec

package object analyzer {

  type Rule[T] = T => ValidatedNec[String, String]

}
