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

package org.apache.carbondata.spark

import scala.collection.mutable.HashMap
import org.apache.carbondata.core.carbon.{CarbonTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.datastorage.store.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.spark.filefilter.DictCarbonFileFilter

object DictionaryDetailHelper {
  def getDictionaryDetail(
      dictFolderPath: String,
      primDimensions: Array[CarbonDimension],
      table: CarbonTableIdentifier,
      storePath: String,
      useSparkTablePath: Boolean): DictionaryDetail = {
    val dictFilePaths = new Array[String](primDimensions.length)
    val dictFileExists = new Array[Boolean](primDimensions.length)
    val columnIdentifier = new Array[ColumnIdentifier](primDimensions.length)

    val fileType = FileFactory.getFileType(dictFolderPath)
    // Metadata folder
    val metadataDirectory = FileFactory.getCarbonFile(dictFolderPath, fileType)
    // need list all dictionary file paths with exists flag
    val carbonFiles = metadataDirectory.listFiles(new DictCarbonFileFilter())
    // 2 put dictionary file names to fileNamesMap
    val fileNamesMap = new HashMap[String, Int]
    for (i <- 0 until carbonFiles.length) {
      fileNamesMap.put(carbonFiles(i).getName, i)
    }
    // 3 lookup fileNamesMap, if file name is in fileNamesMap, file is exists, or not.
    primDimensions.zipWithIndex.foreach { f =>
      columnIdentifier(f._2) = f._1.getColumnIdentifier
      if (useSparkTablePath) {
        dictFilePaths(f._2) = CarbonTablePath.getDictionaryFilePath(storePath, f._1.getColumnId)
        dictFileExists(f._2) =
          fileNamesMap.get(CarbonTablePath.getDictionaryFileName(f._1.getColumnId)) match {
            case None => false
            case Some(_) => true
          }
      } else {
        val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, table)
        dictFilePaths(f._2) = carbonTablePath.getDictionaryFilePath(f._1.getColumnId)
        dictFileExists(f._2) =
          fileNamesMap.get(CarbonTablePath.getDictionaryFileName(f._1.getColumnId)) match {
            case None => false
            case Some(_) => true
          }
      }
    }

    DictionaryDetail(columnIdentifier, dictFilePaths, dictFileExists)
  }
}
