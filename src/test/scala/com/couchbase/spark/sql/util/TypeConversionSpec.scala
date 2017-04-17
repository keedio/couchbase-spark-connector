package com.couchbase.spark.sql.util

import java.lang.Byte
import java.math.BigDecimal

import org.apache.spark.sql.types._
import org.scalatest._
import TypeConversionUtils.{convert, convertTypeIfNeeded}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
  * Created by luca on 11/01/17.
  */
class TypeConversionSpec extends FlatSpec with Matchers {

  val byteVal = new Byte("1")
  val shortVal = new java.lang.Short("1")
  val intVal = new java.lang.Integer("1")
  val longVal = new java.lang.Long("1")
  val floatVal = 1f
  val doubleVal = 1d

  val exampleSchema =
    StructType(
      StructField("uuid", StringType) ::
        StructField("x0", DoubleType) ::
        StructField("x1", DecimalType(25,1)) ::
        StructField("x2", ByteType) ::
        StructField("x3", ByteType) ::
        StructField("x4", DecimalType(25,5)) :: Nil
    )
  
  println(exampleSchema)

  val exampleRow = 
    new GenericRowWithSchema(Array("uuid0", 412, -1244.3d, 2189, null), exampleSchema)
  
  "converting to a Byte" should "throw IllegalArgumentException" in {
    try {
      convert("anyvalue", "tinyint")

      fail("Should not convert successfully")
    } catch {
      case e: IllegalArgumentException =>
      case _ => fail("Should not throw any other exception than IllegalArgumentException")
    }
  }

  "converting a Byte to a Short" should "be successfull" in {
    val converted = convert[java.lang.Short](byteVal.toString, ShortType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Short]))
    assert(converted == 1)
  }

  "converting a Byte to an Integer" should "be successfull" in {
    val converted = convert[java.lang.Integer](byteVal.toString, IntegerType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Integer]))
    assert(converted == 1)
  }

  "converting a Byte to a Long" should "be successfull" in {
    val converted = convert[java.lang.Long](byteVal.toString, LongType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Long]))
    assert(converted == 1)
  }

  "converting a Byte to a Float" should "be successfull" in {
    val converted = convert[java.lang.Float](byteVal.toString, FloatType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Float]))
    assert(converted == 1.0f)
  }

  "converting a Byte to a Double" should "be successfull" in {
    val converted = convert[java.lang.Double](byteVal.toString, DoubleType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Double]))
    assert(converted == 1.0d)
  }

  "converting a Byte to a BigDecimal" should "be successfull" in {
    val converted = convert[BigDecimal](byteVal.toString, "decimal(25,4)")

    assert(converted.getClass.equals(classOf[BigDecimal]))
    assert(converted.scale() == 4)
    assert(converted.compareTo(new BigDecimal("1")) == 0)
  }

  "converting a Short to an Integer" should "be successfull" in {
    val converted = convert[java.lang.Integer](shortVal.toString, IntegerType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Integer]))
    assert(converted == 1)
  }

  "converting a Short to a Long" should "be successfull" in {
    val converted = convert[java.lang.Long](shortVal.toString, LongType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Long]))
    assert(converted == 1)
  }

  "converting a Short to a Float" should "be successfull" in {
    val converted = convert[java.lang.Float](shortVal.toString, FloatType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Float]))
    assert(converted == 1.0f)
  }

  "converting a Short to a Double" should "be successfull" in {
    val converted = convert[java.lang.Double](shortVal.toString, DoubleType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Double]))
    assert(converted == 1.0d)
  }

  "converting a Short to a BigDecimal" should "be successfull" in {
    val converted = convert[BigDecimal](shortVal.toString, "decimal(10,4)")

    assert(converted.getClass.equals(classOf[BigDecimal]))
    assert(converted.scale() == 4)
    assert(converted.compareTo(new BigDecimal("1")) == 0)
  }


  "converting an Integer to a Long" should "be successfull" in {
    val converted = convert[java.lang.Long](intVal.toString, LongType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Long]))
    assert(converted == 1)
  }

  "converting an Integer to a Float" should "be successfull" in {
    val converted = convert[java.lang.Float](intVal.toString, FloatType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Float]))
    assert(converted == 1.0f)
  }

  "converting an Integer to a Double" should "be successfull" in {
    val converted = convert[java.lang.Double](intVal.toString, DoubleType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Double]))
    assert(converted == 1.0d)
  }

  "converting an Integer to a BigDecimal" should "be successfull" in {
    val converted = convert[BigDecimal](intVal.toString, "decimal(6,5)")

    assert(converted.getClass.equals(classOf[BigDecimal]))
    assert(converted.scale() == 5)
    assert(converted.compareTo(new BigDecimal("1")) == 0)
  }


  "converting a Long to a Float" should "be successfull" in {
    val converted = convert[java.lang.Float](longVal.toString, FloatType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Float]))
    assert(converted == 1.0f)
  }

  "converting a Long to a Double" should "be successfull" in {
    val converted = convert[java.lang.Double](longVal.toString, DoubleType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Double]))
    assert(converted == 1.0d)
  }

  "converting a Long to a BigDecimal" should "be successfull" in {
    val converted = convert[BigDecimal](longVal.toString, "decimal(10,4)")

    assert(converted.getClass.equals(classOf[BigDecimal]))
    assert(converted.scale() == 4)
    assert(converted.compareTo(new BigDecimal("1")) == 0)
  }


  "converting a Float to a Double" should "be successfull" in {
    val converted = convert[java.lang.Double](floatVal.toString, DoubleType.simpleString)

    assert(converted.getClass.equals(classOf[java.lang.Double]))
    assert(converted == 1.0d)
  }

  "converting a Float to a BigDecimal" should "be successfull" in {
    val converted = convert[BigDecimal](floatVal.toString, "decimal(10,4)")

    assert(converted.getClass.equals(classOf[BigDecimal]))
    assert(converted.scale() == 4)
    assert(converted.compareTo(new BigDecimal("1")) == 0)
  }
  
  "converting a Double to a BigDecimal" should "be successfull" in {
    val converted = convert[BigDecimal](doubleVal.toString, "decimal(9,5)")

    assert(converted.getClass.equals(classOf[BigDecimal]))
    assert(converted.scale() == 5)
    assert(converted.compareTo(new BigDecimal("1")) == 0)
  }

  "convertTypeIfNeeded" should "return null if field does not exist" in {
    try {
      convertTypeIfNeeded(exampleRow, "notExistent", exampleSchema)
      fail("Should not normalize successfully")
    } catch {
      case e: IllegalArgumentException =>
      case _ => fail("Should not throw any other exception than IllegalArgumentException")
    }
  }

  it should "return null if content is null" in {
    val converted = convertTypeIfNeeded(exampleRow, "x3", exampleSchema)

    // reference comparison, I want to check I am referencing the same object
    assert(converted == null)
  }

  it should "return the unconverted value if not necessary" in {
    val converted = convertTypeIfNeeded(exampleRow, "uuid", exampleSchema)

    val fieldIdx = exampleRow.fieldIndex("uuid")
    
    // reference comparison, I want to check I am referencing the same object
    assert(converted == exampleRow.get(fieldIdx))
  }

  it should "return the unconverted value if type is not convertible" in {
    val converted = convertTypeIfNeeded(exampleRow, "x2", exampleSchema)

    val fieldIdx = exampleRow.fieldIndex("x2")

    // reference comparison, I want to check I am referencing the same object
    assert(converted == exampleRow.get(fieldIdx))
  }

  it should "return the converted value if type is convertible" in {
    val converted0 = convertTypeIfNeeded(exampleRow, "x0", exampleSchema)

    // reference comparison, I want to check I am referencing the same object
    assert(converted0.getClass.equals(classOf[java.lang.Double]))
    assert(converted0 == 412.0d)

    val converted1 = convertTypeIfNeeded(exampleRow, "x1", exampleSchema)

    // reference comparison, I want to check I am referencing the same object
    assert(converted1.getClass.equals(classOf[java.math.BigDecimal]))
    assert(converted1.equals(new BigDecimal("-1244.3")))
  }
}
