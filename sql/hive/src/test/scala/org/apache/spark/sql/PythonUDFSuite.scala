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

package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.collection.JavaConversions._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.types.{DataType, StructType}

class PythonUDFSuite extends QueryTest {
  import org.apache.spark.sql.hive.test.TestHive._

  val testData = sparkContext.parallelize(
    (1 to 100).map(i => TestData(i, i.toString)))
  testData.registerAsTable("testData")

  /* sqlCtx.registerFunction("test", lambda (x): "test:" + x) */
  registerPython(
    "test",
    Array[Byte](-128, 2, 40, 99, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 10, 95, 109, 111, 100, 117, 108, 101, 115, 95, 116, 111, 95, 109, 97, 105, 110, 10, 113, 0, 93, 113, 1, 85, 11, 112, 121, 115, 112, 97, 114, 107, 46, 115, 113, 108, 113, 2, 97, -123, 113, 3, 82, 49, 99, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 10, 95, 102, 105, 108, 108, 95, 102, 117, 110, 99, 116, 105, 111, 110, 10, 113, 4, 40, 99, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 10, 95, 109, 97, 107, 101, 95, 115, 107, 101, 108, 95, 102, 117, 110, 99, 10, 113, 5, 99, 110, 101, 119, 10, 99, 111, 100, 101, 10, 113, 6, 40, 75, 2, 75, 2, 75, 3, 75, 19, 85, 13, 116, 0, 0, -120, 0, 0, 124, 1, 0, -125, 2, 0, 83, 113, 7, 78, -123, 113, 8, 85, 4, 105, 109, 97, 112, 113, 9, -123, 113, 10, 85, 5, 115, 112, 108, 105, 116, 113, 11, 85, 8, 105, 116, 101, 114, 97, 116, 111, 114, 113, 12, -122, 113, 13, 85, 53, 47, 85, 115, 101, 114, 115, 47, 109, 97, 114, 109, 98, 114, 117, 115, 47, 119, 111, 114, 107, 115, 112, 97, 99, 101, 47, 115, 112, 97, 114, 107, 47, 112, 121, 116, 104, 111, 110, 47, 112, 121, 115, 112, 97, 114, 107, 47, 115, 113, 108, 46, 112, 121, 113, 14, 85, 4, 102, 117, 110, 99, 113, 15, 75, 81, 85, 0, 113, 16, 85, 1, 102, 113, 17, -123, 113, 18, 41, 116, 113, 19, 82, 113, 20, 75, 1, 125, 113, 21, -121, 113, 22, 82, 113, 23, 125, 113, 24, 104, 9, 99, 105, 116, 101, 114, 116, 111, 111, 108, 115, 10, 105, 109, 97, 112, 10, 113, 25, 115, 78, 93, 113, 26, 104, 4, 40, 104, 5, 104, 6, 40, 75, 1, 75, 1, 75, 2, 75, 67, 85, 8, 100, 1, 0, 124, 0, 0, 23, 83, 113, 27, 78, 85, 5, 116, 101, 115, 116, 58, 113, 28, -122, 113, 29, 41, 85, 1, 120, 113, 30, -123, 113, 31, 85, 7, 60, 115, 116, 100, 105, 110, 62, 113, 32, 85, 8, 60, 108, 97, 109, 98, 100, 97, 62, 113, 33, 75, 1, 85, 0, 113, 34, 41, 41, 116, 113, 35, 82, 113, 36, 75, 0, 125, 113, 37, -121, 113, 38, 82, 113, 39, 125, 113, 40, 78, 93, 113, 41, 125, 113, 42, 116, 82, 97, 125, 113, 43, 116, 82, 99, 112, 121, 115, 112, 97, 114, 107, 46, 115, 101, 114, 105, 97, 108, 105, 122, 101, 114, 115, 10, 66, 97, 116, 99, 104, 101, 100, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 10, 113, 44, 41, -127, 113, 45, 125, 113, 46, 40, 85, 9, 98, 97, 116, 99, 104, 83, 105, 122, 101, 113, 47, 77, 0, 4, 85, 10, 115, 101, 114, 105, 97, 108, 105, 122, 101, 114, 113, 48, 99, 112, 121, 115, 112, 97, 114, 107, 46, 115, 101, 114, 105, 97, 108, 105, 122, 101, 114, 115, 10, 80, 105, 99, 107, 108, 101, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 10, 113, 49, 41, -127, 113, 50, 125, 113, 51, 85, 19, 95, 111, 110, 108, 121, 95, 119, 114, 105, 116, 101, 95, 115, 116, 114, 105, 110, 103, 115, 113, 52, -119, 115, 98, 117, 98, 104, 45, -121, 113, 53, 46),
    new java.util.HashMap[String, String](),
    new java.util.LinkedList[String](),
    "python",
    null
  )

  /**
   * import re
   * sqlCtx.registerFunction("countMatches", lambda (pattern, string): re.subn(pattern, '', string)[1])
   */
  registerPython(
    "countMatches",
    Array[Byte](-128, 2, 40, 99, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 10, 95, 109, 111, 100, 117, 108, 101, 115, 95, 116, 111, 95, 109, 97, 105, 110, 10, 113, 0, 93, 113, 1, 85, 11, 112, 121, 115, 112, 97, 114, 107, 46, 115, 113, 108, 113, 2, 97, -123, 113, 3, 82, 49, 99, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 10, 95, 102, 105, 108, 108, 95, 102, 117, 110, 99, 116, 105, 111, 110, 10, 113, 4, 40, 99, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 10, 95, 109, 97, 107, 101, 95, 115, 107, 101, 108, 95, 102, 117, 110, 99, 10, 113, 5, 99, 110, 101, 119, 10, 99, 111, 100, 101, 10, 113, 6, 40, 75, 2, 75, 2, 75, 3, 75, 19, 85, 13, 116, 0, 0, -120, 0, 0, 124, 1, 0, -125, 2, 0, 83, 113, 7, 78, -123, 113, 8, 85, 4, 105, 109, 97, 112, 113, 9, -123, 113, 10, 85, 5, 115, 112, 108, 105, 116, 113, 11, 85, 8, 105, 116, 101, 114, 97, 116, 111, 114, 113, 12, -122, 113, 13, 85, 53, 47, 85, 115, 101, 114, 115, 47, 109, 97, 114, 109, 98, 114, 117, 115, 47, 119, 111, 114, 107, 115, 112, 97, 99, 101, 47, 115, 112, 97, 114, 107, 47, 112, 121, 116, 104, 111, 110, 47, 112, 121, 115, 112, 97, 114, 107, 47, 115, 113, 108, 46, 112, 121, 113, 14, 85, 4, 102, 117, 110, 99, 113, 15, 75, 81, 85, 0, 113, 16, 85, 1, 102, 113, 17, -123, 113, 18, 41, 116, 113, 19, 82, 113, 20, 75, 1, 125, 113, 21, -121, 113, 22, 82, 113, 23, 125, 113, 24, 104, 9, 99, 105, 116, 101, 114, 116, 111, 111, 108, 115, 10, 105, 109, 97, 112, 10, 113, 25, 115, 78, 93, 113, 26, 104, 4, 40, 104, 5, 104, 6, 40, 75, 1, 75, 3, 75, 4, 75, 67, 85, 35, 124, 0, 0, 92, 2, 0, 125, 1, 0, 125, 2, 0, 116, 0, 0, 106, 1, 0, 124, 1, 0, 100, 1, 0, 124, 2, 0, -125, 3, 0, 100, 2, 0, 25, 83, 113, 27, 78, 104, 16, 75, 1, -121, 113, 28, 85, 2, 114, 101, 113, 29, 85, 4, 115, 117, 98, 110, 113, 30, -122, 113, 31, 85, 2, 46, 48, 113, 32, 85, 7, 112, 97, 116, 116, 101, 114, 110, 113, 33, 85, 6, 115, 116, 114, 105, 110, 103, 113, 34, -121, 113, 35, 85, 7, 60, 115, 116, 100, 105, 110, 62, 113, 36, 85, 8, 60, 108, 97, 109, 98, 100, 97, 62, 113, 37, 75, 1, 85, 2, 3, 0, 113, 38, 41, 41, 116, 113, 39, 82, 113, 40, 75, 0, 125, 113, 41, -121, 113, 42, 82, 113, 43, 125, 113, 44, 104, 29, 99, 112, 121, 115, 112, 97, 114, 107, 46, 99, 108, 111, 117, 100, 112, 105, 99, 107, 108, 101, 10, 115, 117, 98, 105, 109, 112, 111, 114, 116, 10, 113, 45, 85, 2, 114, 101, 113, 46, -123, 113, 47, 82, 113, 48, 115, 78, 93, 113, 49, 125, 113, 50, 116, 82, 97, 125, 113, 51, 116, 82, 99, 112, 121, 115, 112, 97, 114, 107, 46, 115, 101, 114, 105, 97, 108, 105, 122, 101, 114, 115, 10, 66, 97, 116, 99, 104, 101, 100, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 10, 113, 52, 41, -127, 113, 53, 125, 113, 54, 40, 85, 9, 98, 97, 116, 99, 104, 83, 105, 122, 101, 113, 55, 77, 0, 4, 85, 10, 115, 101, 114, 105, 97, 108, 105, 122, 101, 114, 113, 56, 99, 112, 121, 115, 112, 97, 114, 107, 46, 115, 101, 114, 105, 97, 108, 105, 122, 101, 114, 115, 10, 80, 105, 99, 107, 108, 101, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 10, 113, 57, 41, -127, 113, 58, 125, 113, 59, 85, 19, 95, 111, 110, 108, 121, 95, 119, 114, 105, 116, 101, 95, 115, 116, 114, 105, 110, 103, 115, 113, 60, -119, 115, 98, 117, 98, 104, 53, -121, 113, 61, 46),
    new java.util.HashMap[String, String](),
    new java.util.LinkedList[String](),
    "python",
    null
  )


  test("Single argument UDF") {
   checkAnswer(
     hql("SELECT test(value) FROM testData"),
     testData.select('value).map(r =>
       new GenericRow(Array[Any]("test:" + r(0).toString))).collect().toSeq)
  }

  test("Multiple argument UDF") {
    checkAnswer(
      hql("SELECT countMatches('1', value) FROM testData"),
      testData.select('value).map(r =>
        new GenericRow(Array[Any](
          "1".r.findAllMatchIn(r(0).toString).length.toString))).collect().toSeq)
  }

  test("UDF in WHERE") {
    checkAnswer(
      hql("SELECT value FROM testData WHERE countMatches('1', value) >= 1"),
      testData
        .select('value)
        .filter(r => "1".r.findAllMatchIn(r(0).toString).length >= 1)
        .collect().toSeq)
  }

  test("Integer Return Type") {

  }
}
