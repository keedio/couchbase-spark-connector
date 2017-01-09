/*
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.spark.sql

import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark.connection.CouchbaseConfig
import com.couchbase.spark.rdd.QueryRDD
import com.couchbase.spark.sql.N1QLRelation.buildColumns
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._

/**
  * Implements a the BaseRelation for N1QL Queries.
  *
  * @param bucket     the name of the bucket
  * @param userSchema the optional schema (if not provided it will be inferred)
  * @param sqlContext the sql context.
  */
class N1QLRelation(bucket: String, userSchema: Option[StructType], parameters: Map[String, String])
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  override val schema = userSchema.getOrElse[StructType] {
    val queryFilter = if (parameters.get("schemaFilter").isDefined) {
      "WHERE " + parameters.get("schemaFilter").get
    } else {
      ""
    }

    val query = s"SELECT META(`$bucketName`).id as `$idFieldName`, `$bucketName`.* " +
      s"FROM `$bucketName` $queryFilter LIMIT 1000"

    logInfo(s"Inferring schema from bucket $bucketName with query '$query'")

    val schema = sqlContext.read.json(
      QueryRDD(sqlContext.sparkContext, bucketName, N1qlQuery.simple(query)).map(_.value.toString)
    ).schema

    logInfo(s"Inferred schema is $schema")

    schema
  }
  private val cbConfig = CouchbaseConfig(sqlContext.sparkContext.getConf)
  private val bucketName = Option(bucket).getOrElse(cbConfig.buckets.head.name)
  private val idFieldName = parameters.getOrElse("idField", DefaultSource.DEFAULT_DOCUMENT_ID_FIELD)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var stringFilter = buildFilter(filters)
    if (parameters.get("schemaFilter").isDefined) {
      if (!stringFilter.isEmpty) {
        stringFilter = stringFilter + " AND "
      }
      stringFilter += parameters.get("schemaFilter").get
    }

    if (!stringFilter.isEmpty) {
      stringFilter = " WHERE " + stringFilter
    }

    val query = "SELECT " +
      buildColumns(requiredColumns, bucketName, idFieldName, cbConfig.nullIfMissing) +
      " FROM `" +
      bucketName + "`" + stringFilter

    logInfo(s"Executing generated query: '$query'")

    val mySchema = schema.json

    sqlContext.read.json(
      QueryRDD(sqlContext.sparkContext, bucketName, N1qlQuery.simple(query)).map(elem => {

        elem.value.toString
      })
    ).mapPartitions(iter => {
      val schema = DataType.fromJson(mySchema) match {
        case t: StructType => t
        case _ => throw new RuntimeException(s"Failed parsing StructType: $mySchema")
      }

      iter.map {
        row =>
          Row.fromSeq(
            requiredColumns.map {
              col =>
                val fieldIdx = row.fieldIndex(col)

                if (row.get(fieldIdx) != null) {
                  val structField = schema.filter(f => f.name.equals(col)).head
                  val fieldType = row.get(fieldIdx).getClass

                  if (structField.dataType.isInstanceOf[DoubleType] &&
                    !(fieldType.equals(classOf[java.lang.Double]) ||
                      fieldType.equals(classOf[scala.Double]))) {

                    java.lang.Double.parseDouble(row.get(fieldIdx).toString)
                  } else {
                    row.get(fieldIdx)
                  }
                } else {
                  null
                }
            })
      }
    })
  }

  /**
    * Transform the filters into a N1QL where clause.
    *
    * @todo In, And, Or, Not filters including recursion
    * @param filters the filters to transform
    * @return the transformed raw N1QL clause
    */
  private def buildFilter(filters: Array[Filter]): String = {
    if (filters.isEmpty) {
      return ""
    }

    val filter = new StringBuilder()
    var i = 0

    filters.foreach(f => {
      try {
        val encoded = N1QLRelation.filterToExpression(f)
        if (i > 0) {
          filter.append(" AND")
        }
        filter.append(encoded)
        i = i + 1
      } catch {
        case _: Exception => logInfo("Ignoring unsupported filter: " + f)
      }
    })

    filter.toString()
  }

}

object N1QLRelation {
  val VerbatimRegex = """'(.*)'""".r

  /**
    * Transforms the required columns into the field list for the select statement.
    *
    * @param requiredColumns the columns to transform.
    * @return the raw N1QL string
    */
  def buildColumns(requiredColumns: Array[String],
                   bucketName: String,
                   idFieldName: String,
                   nullIfMissing: Boolean = false): String = {
    if (requiredColumns.isEmpty) {
      return s"`$bucketName`.*"
    }

    requiredColumns
      .map(column => {
        if (column == idFieldName) {
          s"META(`$bucketName`).id as `$idFieldName`"
        } else if (nullIfMissing) {
          s"IFMISSING(`$column`,NULL) as `$column`"
        } else {
          "`" + column + "`"
        }
      })
      .mkString(",")
  }

  /**
    * Turns a filter into a N1QL expression.
    *
    * @param filter the filter to convert
    * @return the resulting expression
    */
  def filterToExpression(filter: Filter): String = {
    filter match {
      case EqualTo(attr, value) => s" ${attrToFilter(attr)} = " + valueToFilter(value)
      case GreaterThan(attr, value) => s" ${attrToFilter(attr)} > " + valueToFilter(value)
      case GreaterThanOrEqual(attr, value) => s" ${attrToFilter(attr)} >= " + valueToFilter(value)
      case LessThan(attr, value) => s" ${attrToFilter(attr)} < " + valueToFilter(value)
      case LessThanOrEqual(attr, value) => s" ${attrToFilter(attr)} <= " + valueToFilter(value)
      case IsNull(attr) => s" ${attrToFilter(attr)} IS NULL"
      case IsNotNull(attr) => s" ${attrToFilter(attr)} IS NOT NULL"
      case StringContains(attr, value) => s" CONTAINS(${attrToFilter(attr)}, '$value')"
      case StringStartsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '" + escapeForLike(value) + "%'"
      case StringEndsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '%" + escapeForLike(value) + "'"
      case In(attr, values) =>
        val encoded = values.map(valueToFilter).mkString(",")
        s" `$attr` IN [$encoded]"
      case And(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l AND $r)"
      case Or(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l OR $r)"
      case Not(f) =>
        val v = filterToExpression(f)
        s" NOT ($v)"
    }
  }

  def escapeForLike(value: String): String =
    value.replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\\\*")

  def valueToFilter(value: Any): String = value match {
    case v: String => s"'$v'"
    case v => s"$v"
  }

  def attrToFilter(attr: String): String = attr match {
    case VerbatimRegex(innerAttr) => innerAttr
    case v => v.split('.').map(elem => s"`$elem`").mkString(".")
  }

}
