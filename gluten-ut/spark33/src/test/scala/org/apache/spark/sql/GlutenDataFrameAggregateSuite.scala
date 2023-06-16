/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import io.glutenproject.execution.HashAggregateExecBaseTransformer
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.functions._

class GlutenDataFrameAggregateSuite extends DataFrameAggregateSuite with GlutenSQLTestsTrait {

  import testImplicits._

  // blackTestNameList is defined in ClickHouseNotSupport

  test(GlutenTestConstants.GLUTEN_TEST + "count") {
    // agg with no input col
    assert(testData2.count() === testData2.rdd.map(_ => 1).count())

    checkAnswer(
      testData2.agg(count($"a"), sum_distinct($"a")), // non-partial
      Row(6, 6.0))
  }

  test(GlutenTestConstants.GLUTEN_TEST + "null count") {
    checkAnswer(testData3.groupBy($"a").agg(count($"b")), Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(testData3.groupBy($"a").agg(count($"a" + $"b")), Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(
      testData3
        .agg(count($"a"), count($"b"), count(lit(1)), count_distinct($"a"), count_distinct($"b")),
      Row(2, 1, 2, 2, 1))

    // [wishlist] does not support sum distinct
//    checkAnswer(
//      testData3.agg(count($"b"), count_distinct($"b"), sum_distinct($"b")), // non-partial
//      Row(1, 1, 2)
//    )
  }

  test(GlutenTestConstants.GLUTEN_TEST + "groupBy") {
    checkAnswer(testData2.groupBy("a").agg(sum($"b")), Seq(Row(1, 3), Row(2, 3), Row(3, 3)))
    checkAnswer(testData2.groupBy("a").agg(sum($"b").as("totB")).agg(sum($"totB")), Row(9))
    checkAnswer(
      testData2.groupBy("a").agg(count("*")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil)
    checkAnswer(
      testData2.groupBy("a").agg(Map("*" -> "count")),
      Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil)
    checkAnswer(
      testData2.groupBy("a").agg(Map("b" -> "sum")),
      Row(1, 3) :: Row(2, 3) :: Row(3, 3) :: Nil)

    val df1 = Seq(("a", 1, 0, "b"), ("b", 2, 4, "c"), ("a", 2, 3, "d"))
      .toDF("key", "value1", "value2", "rest")

    checkAnswer(df1.groupBy("key").min(), df1.groupBy("key").min("value1", "value2").collect())
    checkAnswer(df1.groupBy("key").min("value2"), Seq(Row("a", 0), Row("b", 4)))

    // [wishlist] does not support decimal
//    checkAnswer(
//      decimalData.groupBy("a").agg(sum("b")),
//      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(3)),
//        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(3)),
//        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)))
//    )
//
//    val decimalDataWithNulls = spark.sparkContext.parallelize(
//      DecimalData(1, 1) ::
//        DecimalData(1, null) ::
//        DecimalData(2, 1) ::
//        DecimalData(2, null) ::
//        DecimalData(3, 1) ::
//        DecimalData(3, 2) ::
//        DecimalData(null, 2) :: Nil).toDF()
//    checkAnswer(
//      decimalDataWithNulls.groupBy("a").agg(sum("b")),
//      Seq(Row(new java.math.BigDecimal(1), new java.math.BigDecimal(1)),
//        Row(new java.math.BigDecimal(2), new java.math.BigDecimal(1)),
//        Row(new java.math.BigDecimal(3), new java.math.BigDecimal(3)),
//        Row(null, new java.math.BigDecimal(2)))
//    )
  }

  test(GlutenTestConstants.GLUTEN_TEST + "average") {

    checkAnswer(testData2.agg(avg($"a"), mean($"a")), Row(2.0, 2.0))

    checkAnswer(
      testData2.agg(avg($"a"), sumDistinct($"a")), // non-partial and test deprecated version
      Row(2.0, 6.0) :: Nil)

    // [wishlist] does not support decimal
//    checkAnswer(
//      decimalData.agg(avg($"a")),
//      Row(new java.math.BigDecimal(2)))
//
//    checkAnswer(
//      decimalData.agg(avg($"a"), sum_distinct($"a")), // non-partial
//      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)
//
//    checkAnswer(
//      decimalData.agg(avg($"a" cast DecimalType(10, 2))),
//      Row(new java.math.BigDecimal(2)))
//    // non-partial
//    checkAnswer(
//      decimalData.agg(
//        avg($"a" cast DecimalType(10, 2)), sum_distinct($"a" cast DecimalType(10, 2))),
//      Row(new java.math.BigDecimal(2), new java.math.BigDecimal(6)) :: Nil)
  }

  ignore("gluten SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate") {
    withTempView("view") {
      Seq(("mithunr", Float.NaN),
        ("mithunr", Float.NaN),
        ("mithunr", Float.NaN),
        ("abellina", 1.0f),
        ("abellina", 2.0f)).toDF("uid", "score").createOrReplaceTempView("view")

      val df = spark.sql("select uid, count(distinct score) from view group by 1 order by 1 asc")
      checkAnswer(df, Row("abellina", 2) :: Row("mithunr", 1) :: Nil)
    }
  }

  test(GlutenTestConstants.GLUTEN_TEST + "variance") {
    checkAnswer(
      testData2.agg(var_samp($"a"), var_pop($"a"), variance($"a")),
      Row(0.8, 2.0 / 3.0, 0.8))
    checkAnswer(
      testData2.agg(var_samp("a"), var_pop("a"), variance("a")),
      Row(0.8, 2.0 / 3.0, 0.8))
  }

  test("aggregation with filter") {
    Seq(
      ("mithunr", 12.3f, 5.0f, true, 9.4f),
      ("mithunr", 15.5f, 4.0f, false, 19.9f),
      ("mithunr", 19.8f, 3.0f, false, 35.6f),
      ("abellina", 20.1f, 2.0f, true, 98.0f),
      ("abellina", 20.1f, 1.0f, true, 0.5f),
      ("abellina", 23.6f, 2.0f, true, 3.9f))
      .toDF("uid", "time", "score", "pass", "rate")
      .createOrReplaceTempView("view")
    var df = spark.sql("select count(score) filter (where pass) from view group by time")
    checkAnswer(df, Row(1) :: Row(0) :: Row(0) :: Row(2) :: Row(1) :: Nil)

    df = spark.sql("select count(score) filter (where pass) from view")
    checkAnswer(df, Row(4) :: Nil)

    df = spark.sql("select count(score) filter (where rate > 20) from view group by time")
    checkAnswer(df, Row(0) :: Row(0) :: Row(1) :: Row(1) :: Row(0) :: Nil)

    df = spark.sql("select count(score) filter (where rate > 20) from view")
    checkAnswer(df, Row(2) :: Nil)
  }

  test(GlutenTestConstants.GLUTEN_TEST + "extend with cast expression") {
    checkAnswer(
      decimalData.agg(
          sum($"a".cast("double")),
          avg($"b".cast("double")),
          count_distinct($"a"),
          count_distinct($"b")),
      Row(12.0, 1.5, 3, 2))
  }

  // This test is applicable to velox backend. For CH backend, the replacement is disabled.
  test(GlutenTestConstants.GLUTEN_TEST
      + "use gluten hash agg to replace vanilla spark sort agg") {

    withSQLConf(("spark.gluten.sql.columnar.force.hashagg", "false")) {
      Seq("A", "B", "C", "D").toDF("col1").createOrReplaceTempView("t1")
      // SortAggregateExec is expected to be used for string type input.
      val df = spark.sql("select max(col1) from t1")
      checkAnswer(df, Row("D") :: Nil)
      assert(find(df.queryExecution.executedPlan)(
        _.isInstanceOf[SortAggregateExec]).isDefined)
    }

    withSQLConf(("spark.gluten.sql.columnar.force.hashagg", "true")) {
      Seq("A", "B", "C", "D").toDF("col1").createOrReplaceTempView("t1")
      val df = spark.sql("select max(col1) from t1")
      checkAnswer(df, Row("D") :: Nil)
      // Sort agg is expected to be replaced by gluten's hash agg.
      assert(find(df.queryExecution.executedPlan)(
        _.isInstanceOf[HashAggregateExecBaseTransformer]).isDefined)
    }
  }
}