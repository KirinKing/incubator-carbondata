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

package org.apache.spark.sql.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.CarbonLateDecodeStrategy

object CarbonExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .master("local")
        .appName("CarbonExample")
        .enableHiveSupport()
        .getOrCreate()

    val csvPath = ""
    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table
         | USING org.apache.spark.sql.CarbonSource
       """.stripMargin)

    spark.sql(
      s"""
         | CREATE TABLE csv_table
         | (ID int,
         | date timestamp,
         | country string,
         | name string,
         | phonetype string,
         | serialname string,
         | salary int)
       """.stripMargin)

//    spark.sql(
//      s"""
//         | LOAD DATA LOCAL INPATH '$csvPath'
//         | INTO TABLE csv_table
//       """.stripMargin)

    spark.sql(
      s"""
         | INSERT INTO TABLE carbon_table
         | SELECT * FROM csv_table
       """.stripMargin)

    // Perform a query
    spark.sql("""
           SELECT country, count(salary) AS amount
           FROM carbon_table
           WHERE country IN ('china','france')
           GROUP BY country
           """).show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")
  }

}
