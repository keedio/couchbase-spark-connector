package com.couchbase.spark.sql.util

import org.scalatest.{FlatSpec, Matchers}
import TypeConversionUtils.isSameType
import org.apache.spark.sql.types._

/**
  * Created by luca on 11/01/17.
  */
class TypeCompatibilitySpec extends FlatSpec with Matchers {
  ByteType.simpleString must "be a byte"  in {
    assert(isSameType(ByteType.simpleString, classOf[Byte]))
    assert(isSameType(ByteType.simpleString, classOf[java.lang.Byte]))
  }
  
  it must "be different than Short"  in {
    assert(!isSameType(ByteType.simpleString, classOf[Short]))
    assert(!isSameType(ByteType.simpleString, classOf[java.lang.Short]))
  }

  it must "be different than Int"  in {
    assert(!isSameType(ByteType.simpleString, classOf[Int]))
    assert(!isSameType(ByteType.simpleString, classOf[java.lang.Integer]))
  }

  it must "be different than Long"  in {
    assert(!isSameType(ByteType.simpleString, classOf[Long]))
    assert(!isSameType(ByteType.simpleString, classOf[java.lang.Long]))
  }

  it must "be different than Float"  in {
    assert(!isSameType(ByteType.simpleString, classOf[Float]))
    assert(!isSameType(ByteType.simpleString, classOf[java.lang.Float]))
  }

  it must "be different than Double"  in {
    assert(!isSameType(ByteType.simpleString, classOf[Double]))
    assert(!isSameType(ByteType.simpleString, classOf[java.lang.Double]))
  }

  it must "be different than BigDecimal"  in {
    assert(!isSameType(ByteType.simpleString, classOf[BigDecimal]))
    assert(!isSameType(ByteType.simpleString, classOf[java.math.BigDecimal]))
  }

  ShortType.simpleString must "be different than a byte"  in {
    assert(!isSameType(ShortType.simpleString, classOf[Byte]))
    assert(!isSameType(ShortType.simpleString, classOf[java.lang.Byte]))
  }

  it must "be a Short"  in {
    assert(isSameType(ShortType.simpleString, classOf[Short]))
    assert(isSameType(ShortType.simpleString, classOf[java.lang.Short]))
  }

  it must "be different than Int"  in {
    assert(!isSameType(ShortType.simpleString, classOf[Int]))
    assert(!isSameType(ShortType.simpleString, classOf[java.lang.Integer]))
  }

  it must "be different than Long"  in {
    assert(!isSameType(ShortType.simpleString, classOf[Long]))
    assert(!isSameType(ShortType.simpleString, classOf[java.lang.Long]))
  }

  it must "be different than Float"  in {
    assert(!isSameType(ShortType.simpleString, classOf[Float]))
    assert(!isSameType(ShortType.simpleString, classOf[java.lang.Float]))
  }

  it must "be different than Double"  in {
    assert(!isSameType(ShortType.simpleString, classOf[Double]))
    assert(!isSameType(ShortType.simpleString, classOf[java.lang.Double]))
  }

  it must "be different than BigDecimal"  in {
    assert(!isSameType(ShortType.simpleString, classOf[BigDecimal]))
    assert(!isSameType(ShortType.simpleString, classOf[java.math.BigDecimal]))
  }

  IntegerType.simpleString must "be different than a byte"  in {
    assert(!isSameType(IntegerType.simpleString, classOf[Byte]))
    assert(!isSameType(IntegerType.simpleString, classOf[java.lang.Byte]))
  }

  it must "be different than a Short"  in {
    assert(!isSameType(IntegerType.simpleString, classOf[Short]))
    assert(!isSameType(IntegerType.simpleString, classOf[java.lang.Short]))
  }

  it must "be a Int"  in {
    assert(isSameType(IntegerType.simpleString, classOf[Int]))
    assert(isSameType(IntegerType.simpleString, classOf[java.lang.Integer]))
  }

  it must "be different than Long"  in {
    assert(!isSameType(IntegerType.simpleString, classOf[Long]))
    assert(!isSameType(IntegerType.simpleString, classOf[java.lang.Long]))
  }

  it must "be different than Float"  in {
    assert(!isSameType(IntegerType.simpleString, classOf[Float]))
    assert(!isSameType(IntegerType.simpleString, classOf[java.lang.Float]))
  }

  it must "be different than Double"  in {
    assert(!isSameType(IntegerType.simpleString, classOf[Double]))
    assert(!isSameType(IntegerType.simpleString, classOf[java.lang.Double]))
  }

  it must "be different than BigDecimal"  in {
    assert(!isSameType(IntegerType.simpleString, classOf[BigDecimal]))
    assert(!isSameType(IntegerType.simpleString, classOf[java.math.BigDecimal]))
  }

  LongType.simpleString must "be different than a byte"  in {
    assert(!isSameType(LongType.simpleString, classOf[Byte]))
    assert(!isSameType(LongType.simpleString, classOf[java.lang.Byte]))
  }

  it must "be different than a Short"  in {
    assert(!isSameType(LongType.simpleString, classOf[Short]))
    assert(!isSameType(LongType.simpleString, classOf[java.lang.Short]))
  }

  it must "be different than a Int"  in {
    assert(!isSameType(LongType.simpleString, classOf[Int]))
    assert(!isSameType(LongType.simpleString, classOf[java.lang.Integer]))
  }

  it must "be a Long"  in {
    assert(isSameType(LongType.simpleString, classOf[Long]))
    assert(isSameType(LongType.simpleString, classOf[java.lang.Long]))
  }

  it must "be different than Float"  in {
    assert(!isSameType(LongType.simpleString, classOf[Float]))
    assert(!isSameType(LongType.simpleString, classOf[java.lang.Float]))
  }

  it must "be different than Double"  in {
    assert(!isSameType(LongType.simpleString, classOf[Double]))
    assert(!isSameType(LongType.simpleString, classOf[java.lang.Double]))
  }

  it must "be different than BigDecimal"  in {
    assert(!isSameType(LongType.simpleString, classOf[BigDecimal]))
    assert(!isSameType(LongType.simpleString, classOf[java.math.BigDecimal]))
  }

  FloatType.simpleString must "be different than a byte"  in {
    assert(!isSameType(FloatType.simpleString, classOf[Byte]))
    assert(!isSameType(FloatType.simpleString, classOf[java.lang.Byte]))
  }

  it must "be different than a Short"  in {
    assert(!isSameType(FloatType.simpleString, classOf[Short]))
    assert(!isSameType(FloatType.simpleString, classOf[java.lang.Short]))
  }

  it must "be different than a Int"  in {
    assert(!isSameType(FloatType.simpleString, classOf[Int]))
    assert(!isSameType(FloatType.simpleString, classOf[java.lang.Integer]))
  }

  it must "be different than a Long"  in {
    assert(!isSameType(FloatType.simpleString, classOf[Long]))
    assert(!isSameType(FloatType.simpleString, classOf[java.lang.Long]))
  }

  it must "be a Float"  in {
    assert(isSameType(FloatType.simpleString, classOf[Float]))
    assert(isSameType(FloatType.simpleString, classOf[java.lang.Float]))
  }

  it must "be different than Double"  in {
    assert(!isSameType(FloatType.simpleString, classOf[Double]))
    assert(!isSameType(FloatType.simpleString, classOf[java.lang.Double]))
  }

  it must "be different than BigDecimal"  in {
    assert(!isSameType(FloatType.simpleString, classOf[BigDecimal]))
    assert(!isSameType(FloatType.simpleString, classOf[java.math.BigDecimal]))
  }

  DoubleType.simpleString must "be different than a byte"  in {
    assert(!isSameType(DoubleType.simpleString, classOf[Byte]))
    assert(!isSameType(DoubleType.simpleString, classOf[java.lang.Byte]))
  }

  it must "be different than a Short"  in {
    assert(!isSameType(DoubleType.simpleString, classOf[Short]))
    assert(!isSameType(DoubleType.simpleString, classOf[java.lang.Short]))
  }

  it must "be different than a Int"  in {
    assert(!isSameType(DoubleType.simpleString, classOf[Int]))
    assert(!isSameType(DoubleType.simpleString, classOf[java.lang.Integer]))
  }

  it must "be different than a Long"  in {
    assert(!isSameType(DoubleType.simpleString, classOf[Long]))
    assert(!isSameType(DoubleType.simpleString, classOf[java.lang.Long]))
  }

  it must "be different than a Float"  in {
    assert(!isSameType(DoubleType.simpleString, classOf[Float]))
    assert(!isSameType(DoubleType.simpleString, classOf[java.lang.Float]))
  }

  it must "be a Double"  in {
    assert(isSameType(DoubleType.simpleString, classOf[Double]))
    assert(isSameType(DoubleType.simpleString, classOf[java.lang.Double]))
  }

  it must "be different than BigDecimal"  in {
    assert(!isSameType(DoubleType.simpleString, classOf[BigDecimal]))
    assert(!isSameType(DoubleType.simpleString, classOf[java.math.BigDecimal]))
  }

  "decimal" must "be different than a byte"  in {
    assert(!isSameType("decimal", classOf[Byte]))
    assert(!isSameType("decimal", classOf[java.lang.Byte]))
  }

  it must "be different than a Short"  in {
    assert(!isSameType("decimal", classOf[Short]))
    assert(!isSameType("decimal", classOf[java.lang.Short]))
  }

  it must "be different than a Int"  in {
    assert(!isSameType("decimal", classOf[Int]))
    assert(!isSameType("decimal", classOf[java.lang.Integer]))
  }

  it must "be different than a Long"  in {
    assert(!isSameType("decimal", classOf[Long]))
    assert(!isSameType("decimal", classOf[java.lang.Long]))
  }

  it must "be different than a Float"  in {
    assert(!isSameType("decimal", classOf[Float]))
    assert(!isSameType("decimal", classOf[java.lang.Float]))
  }

  it must "be different than a Double"  in {
    assert(!isSameType("decimal", classOf[Double]))
    assert(!isSameType("decimal", classOf[java.lang.Double]))
  }

  it must "be a BigDecimal of any precision/scale"  in {
    assert(isSameType("decimal", classOf[BigDecimal]))
    assert(isSameType("decimal", classOf[java.math.BigDecimal]))
    assert(isSameType("decimal(25,7)", classOf[BigDecimal]))
    assert(isSameType("decimal(25,7)", classOf[java.math.BigDecimal]))
    assert(isSameType("decimal(10,2)", classOf[BigDecimal]))
    assert(isSameType("decimal(10,2)", classOf[java.math.BigDecimal]))
  }
}
