package com.couchbase.spark.sql.util

import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import TypeConversionUtils.isTypeConvertible

/**
  * Created by luca on 11/01/17.
  */
class TypeConvertibilitySpec extends FlatSpec with Matchers {
  "ByteType" should "not be convertible to ByteType" in {
    assert(!isTypeConvertible(ByteType.simpleString, classOf[Byte]))
    assert(!isTypeConvertible(ByteType.simpleString, classOf[java.lang.Byte]))
  }

  it should "be convertible to ShortType" in {
    assert(isTypeConvertible(ShortType.simpleString, classOf[Byte]))
    assert(isTypeConvertible(ShortType.simpleString, classOf[java.lang.Byte]))
  }

  it should "be convertible to IntegerType" in {
    assert(isTypeConvertible(IntegerType.simpleString, classOf[Byte]))
    assert(isTypeConvertible(IntegerType.simpleString, classOf[java.lang.Byte]))
  }

  it should "be convertible to LongType" in {
    assert(isTypeConvertible(LongType.simpleString, classOf[Byte]))
    assert(isTypeConvertible(LongType.simpleString, classOf[java.lang.Byte]))
  }

  it should "be convertible to FloatType" in {
    assert(isTypeConvertible(FloatType.simpleString, classOf[Byte]))
    assert(isTypeConvertible(FloatType.simpleString, classOf[java.lang.Byte]))
  }

  it should "be convertible to DoubleType" in {
    assert(isTypeConvertible(DoubleType.simpleString, classOf[Byte]))
    assert(isTypeConvertible(DoubleType.simpleString, classOf[java.lang.Byte]))
  }

  it should "be convertible to DecimalType with any precision/scale" in {
    assert(isTypeConvertible("decimal", classOf[Byte]))
    assert(isTypeConvertible("decimal", classOf[java.lang.Byte]))
    
    assert(isTypeConvertible("decimal(25,10)", classOf[Byte]))
    assert(isTypeConvertible("decimal(25,10)", classOf[java.lang.Byte]))
  }

  "ShortType" should "not be convertible to ByteType" in {
    assert(!isTypeConvertible(ByteType.simpleString, classOf[Short]))
    assert(!isTypeConvertible(ByteType.simpleString, classOf[java.lang.Short]))
  }

  it should "not be convertible to ShortType" in {
    assert(!isTypeConvertible(ShortType.simpleString, classOf[Short]))
    assert(!isTypeConvertible(ShortType.simpleString, classOf[java.lang.Short]))
  }

  it should "be convertible to IntegerType" in {
    assert(isTypeConvertible(IntegerType.simpleString, classOf[Short]))
    assert(isTypeConvertible(IntegerType.simpleString, classOf[java.lang.Short]))
  }

  it should "be convertible to LongType" in {
    assert(isTypeConvertible(LongType.simpleString, classOf[Short]))
    assert(isTypeConvertible(LongType.simpleString, classOf[java.lang.Short]))
  }

  it should "be convertible to FloatType" in {
    assert(isTypeConvertible(FloatType.simpleString, classOf[Short]))
    assert(isTypeConvertible(FloatType.simpleString, classOf[java.lang.Short]))
  }

  it should "be convertible to DoubleType" in {
    assert(isTypeConvertible(DoubleType.simpleString, classOf[Short]))
    assert(isTypeConvertible(DoubleType.simpleString, classOf[java.lang.Short]))
  }

  it should "be convertible to DecimalType with any precision/scale" in {
    assert(isTypeConvertible("decimal", classOf[Short]))
    assert(isTypeConvertible("decimal", classOf[java.lang.Short]))

    assert(isTypeConvertible("decimal(25,10)", classOf[Short]))
    assert(isTypeConvertible("decimal(25,10)", classOf[java.lang.Short]))
  }

  "IntegerType" should "not be convertible to ByteType" in {
    assert(!isTypeConvertible(ByteType.simpleString, classOf[Int]))
    assert(!isTypeConvertible(ByteType.simpleString, classOf[java.lang.Integer]))
  }

  it should "not be convertible to ShortType" in {
    assert(!isTypeConvertible(ShortType.simpleString, classOf[Int]))
    assert(!isTypeConvertible(ShortType.simpleString, classOf[java.lang.Integer]))
  }

  it should "not be convertible to IntegerType" in {
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[Int]))
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[java.lang.Integer]))
  }

  it should "be convertible to LongType" in {
    assert(isTypeConvertible(LongType.simpleString, classOf[Int]))
    assert(isTypeConvertible(LongType.simpleString, classOf[java.lang.Integer]))
  }

  it should "be convertible to FloatType" in {
    assert(isTypeConvertible(FloatType.simpleString, classOf[Int]))
    assert(isTypeConvertible(FloatType.simpleString, classOf[java.lang.Integer]))
  }

  it should "be convertible to DoubleType" in {
    assert(isTypeConvertible(DoubleType.simpleString, classOf[Int]))
    assert(isTypeConvertible(DoubleType.simpleString, classOf[java.lang.Integer]))
  }

  it should "be convertible to DecimalType with any precision/scale" in {
    assert(isTypeConvertible("decimal", classOf[Int]))
    assert(isTypeConvertible("decimal", classOf[java.lang.Integer]))

    assert(isTypeConvertible("decimal(25,10)", classOf[Int]))
    assert(isTypeConvertible("decimal(25,10)", classOf[java.lang.Integer]))
  }

  "LongType" should "not be convertible to ByteType" in {
    assert(!isTypeConvertible(ByteType.simpleString, classOf[Long]))
    assert(!isTypeConvertible(ByteType.simpleString, classOf[java.lang.Long]))
  }

  it should "not be convertible to ShortType" in {
    assert(!isTypeConvertible(ShortType.simpleString, classOf[Long]))
    assert(!isTypeConvertible(ShortType.simpleString, classOf[java.lang.Long]))
  }

  it should "not be convertible to IntegerType" in {
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[Long]))
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[java.lang.Long]))
  }

  it should "not be convertible to LongType" in {
    assert(!isTypeConvertible(LongType.simpleString, classOf[Long]))
    assert(!isTypeConvertible(LongType.simpleString, classOf[java.lang.Long]))
  }

  it should "be convertible to FloatType" in {
    assert(isTypeConvertible(FloatType.simpleString, classOf[Long]))
    assert(isTypeConvertible(FloatType.simpleString, classOf[java.lang.Long]))
  }

  it should "be convertible to DoubleType" in {
    assert(isTypeConvertible(DoubleType.simpleString, classOf[Long]))
    assert(isTypeConvertible(DoubleType.simpleString, classOf[java.lang.Long]))
  }

  it should "be convertible to DecimalType with any precision/scale" in {
    assert(isTypeConvertible("decimal", classOf[Long]))
    assert(isTypeConvertible("decimal", classOf[java.lang.Long]))

    assert(isTypeConvertible("decimal(25,10)", classOf[Long]))
    assert(isTypeConvertible("decimal(25,10)", classOf[java.lang.Long]))
  }

  "FloatType" should "not be convertible to ByteType" in {
    assert(!isTypeConvertible(ByteType.simpleString, classOf[Float]))
    assert(!isTypeConvertible(ByteType.simpleString, classOf[java.lang.Float]))
  }

  it should "not be convertible to ShortType" in {
    assert(!isTypeConvertible(ShortType.simpleString, classOf[Float]))
    assert(!isTypeConvertible(ShortType.simpleString, classOf[java.lang.Float]))
  }

  it should "not be convertible to IntegerType" in {
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[Float]))
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[java.lang.Float]))
  }

  it should "not be convertible to LongType" in {
    assert(!isTypeConvertible(LongType.simpleString, classOf[Float]))
    assert(!isTypeConvertible(LongType.simpleString, classOf[java.lang.Float]))
  }

  it should "not be convertible to FloatType" in {
    assert(!isTypeConvertible(FloatType.simpleString, classOf[Float]))
    assert(!isTypeConvertible(FloatType.simpleString, classOf[java.lang.Float]))
  }

  it should "be convertible to DoubleType" in {
    assert(isTypeConvertible(DoubleType.simpleString, classOf[Float]))
    assert(isTypeConvertible(DoubleType.simpleString, classOf[java.lang.Float]))
  }

  it should "be convertible to DecimalType with any precision/scale" in {
    assert(isTypeConvertible("decimal", classOf[Float]))
    assert(isTypeConvertible("decimal", classOf[java.lang.Float]))

    assert(isTypeConvertible("decimal(25,10)", classOf[Float]))
    assert(isTypeConvertible("decimal(25,10)", classOf[java.lang.Float]))
  }

  "DoubleType" should "not be convertible to ByteType" in {
    assert(!isTypeConvertible(ByteType.simpleString, classOf[Double]))
    assert(!isTypeConvertible(ByteType.simpleString, classOf[java.lang.Double]))
  }

  it should "not be convertible to ShortType" in {
    assert(!isTypeConvertible(ShortType.simpleString, classOf[Double]))
    assert(!isTypeConvertible(ShortType.simpleString, classOf[java.lang.Double]))
  }

  it should "not be convertible to IntegerType" in {
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[Double]))
    assert(!isTypeConvertible(IntegerType.simpleString, classOf[java.lang.Double]))
  }

  it should "not be convertible to LongType" in {
    assert(!isTypeConvertible(LongType.simpleString, classOf[Double]))
    assert(!isTypeConvertible(LongType.simpleString, classOf[java.lang.Double]))
  }

  it should "not be convertible to FloatType" in {
    assert(!isTypeConvertible(FloatType.simpleString, classOf[Double]))
    assert(!isTypeConvertible(FloatType.simpleString, classOf[java.lang.Double]))
  }

  it should "not be convertible to DoubleType" in {
    assert(!isTypeConvertible(DoubleType.simpleString, classOf[Double]))
    assert(!isTypeConvertible(DoubleType.simpleString, classOf[java.lang.Double]))
  }

  it should "be convertible to DecimalType with any precision/scale" in {
    assert(isTypeConvertible("decimal", classOf[Double]))
    assert(isTypeConvertible("decimal", classOf[java.lang.Double]))

    assert(isTypeConvertible("decimal(25,10)", classOf[Double]))
    assert(isTypeConvertible("decimal(25,10)", classOf[java.lang.Double]))
  }
}
