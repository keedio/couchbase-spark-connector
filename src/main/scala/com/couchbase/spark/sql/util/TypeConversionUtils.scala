package com.couchbase.spark.sql.util

import java.math.{BigDecimal, MathContext}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.matching.Regex

/**
  * Created by luca on 11/01/17.
  */
object TypeConversionUtils {
  
  def convertTypeIfNeeded(row: Row, col: String, schema: StructType): Any = {
    val fieldIdx = row.fieldIndex(col)

    if (row.get(fieldIdx) != null) {
      val structField = schema.filter(f => f.name.equals(col)).head.dataType
      val fieldType = row.get(fieldIdx).getClass

      if (!isSameType(structField.simpleString, fieldType) &&
        isTypeConvertible(structField.simpleString, fieldType)) {

        val colValue = row.get(fieldIdx).toString
        convert(colValue, structField.simpleString)

      } else {
        row.get(fieldIdx)
      }
    } else {
      null
    }
  }

  private val numericTypesCompatibilityMap: Map[Class[_], Array[String]] = Map(
    classOf[Byte] -> Array(ShortType.simpleString, IntegerType.simpleString, LongType.simpleString,
      DoubleType.simpleString, FloatType.simpleString, "decimal"),
    classOf[java.lang.Byte] -> Array(ShortType.simpleString,
      IntegerType.simpleString, LongType.simpleString,
      DoubleType.simpleString, FloatType.simpleString, "decimal"),

    classOf[Short] ->
      Array(IntegerType.simpleString, LongType.simpleString,
        DoubleType.simpleString, FloatType.simpleString, "decimal"),
    classOf[java.lang.Short] ->
      Array(IntegerType.simpleString, LongType.simpleString,
        DoubleType.simpleString, FloatType.simpleString, "decimal"),

    classOf[Int] -> Array(LongType.simpleString, DoubleType.simpleString, 
      FloatType.simpleString, "decimal"),
    classOf[java.lang.Integer] ->
      Array(LongType.simpleString, DoubleType.simpleString, FloatType.simpleString, "decimal"),

    classOf[Long] -> Array(DoubleType.simpleString, FloatType.simpleString, "decimal"),
    classOf[java.lang.Long] -> Array(DoubleType.simpleString, FloatType.simpleString, "decimal"),
    classOf[Float] -> Array(DoubleType.simpleString, "decimal"),
    classOf[java.lang.Float] -> Array(DoubleType.simpleString, "decimal"),
    classOf[Double] -> Array("decimal"),
    classOf[java.lang.Double] -> Array("decimal")
  )


  private val sparkNumericTypeToJavaTypeMap = Map(
    ByteType.simpleString -> Array(classOf[Byte], classOf[java.lang.Byte]),
    ShortType.simpleString -> Array(classOf[Short], classOf[java.lang.Short]),
    IntegerType.simpleString -> Array(classOf[Int], classOf[java.lang.Integer]),
    LongType.simpleString -> Array(classOf[Long], classOf[java.lang.Long]),
    FloatType.simpleString -> Array(classOf[Float], classOf[java.lang.Float]),
    DoubleType.simpleString -> Array(classOf[Double], classOf[java.lang.Double]),
    "decimal" -> Array(classOf[scala.math.BigDecimal], classOf[java.math.BigDecimal])
  )
  
  def convert[T <: Number](value: String, schemaType: String): T = {
    val Pattern: Regex = "decimal\\(([0-9]{1,2}),([0-9]{1,2})\\)".r
    
    schemaType match {
      case "tinyint" => throw new IllegalArgumentException("tinyint not expected here")
      case "smallint" => java.lang.Short.parseShort(value).asInstanceOf[T]
      case "int" => Integer.parseInt(value).asInstanceOf[T]
      case "bigint" => java.lang.Long.parseLong(value).asInstanceOf[T]
      case "float" => java.lang.Float.parseFloat(value).asInstanceOf[T]
      case "double" => java.lang.Double.parseDouble(value).asInstanceOf[T]
      case Pattern(precision, scale) => {
        val mc = new MathContext(precision.toInt)
        new BigDecimal(value, mc).setScale(scale.toInt).asInstanceOf[T]
      }
      case _ => throw new IllegalArgumentException("destination type not recognized")
    }
  }

  def isTypeConvertible(schemaType: String,
                        fieldType: Class[_]): Boolean = {

    val realSchemaType = if (schemaType.startsWith("decimal")) "decimal" else schemaType
    
    numericTypesCompatibilityMap.isDefinedAt(fieldType) &&
      numericTypesCompatibilityMap(fieldType).contains(realSchemaType)
  }

  def isSameType(schemaType: String,
                 fieldType: Class[_]): Boolean = {
    
    val realSchemaType = if (schemaType.startsWith("decimal")) "decimal" else schemaType
    
    sparkNumericTypeToJavaTypeMap.isDefinedAt(realSchemaType) &&
      sparkNumericTypeToJavaTypeMap(realSchemaType).contains(fieldType)
  }
}
