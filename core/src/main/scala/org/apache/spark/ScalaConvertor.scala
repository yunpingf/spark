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
package org.apache.spark

import java.io.ObjectInputStream

import org.apache.spark.storage.{StorageLevel, BlockStatus, BlockId}
import org.apache.spark.util.Utils
import tachyon.TachyonURI
import tachyon.client.file.options.{InStreamOptions, GetInfoOptions}
import tachyon.client.file.{FileInStream, TachyonFileSystem}
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory

import scala.collection.mutable.{HashSet, LinkedHashMap, HashMap}
import scala.collection.immutable.Map
import scala.collection.Set

object ScalaConvertor {
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
  val samplingRates = Array(0.2, 0.3, 0.4)
  type Selectivity = java.util.LinkedHashMap[java.lang.Integer,
    java.util.LinkedHashMap[BlockId, (java.lang.Long, java.lang.Long)]]

  def getNumOfRecords():
  java.util.HashMap[java.lang.Double, java.util.HashMap[BlockId, java.lang.Long]] = {
    val resultMap =
      new java.util.HashMap[java.lang.Double, java.util.HashMap[BlockId, java.lang.Long]]
    for (samplingRate <- samplingRates) {
      if (Utils.tachyonFileExist(TachyonPath.numOfRecords(samplingRate), tfs)){
        val m = new java.util.HashMap[BlockId, java.lang.Long]
        val numOfRecords = Utils.readFromTachyonFile(TachyonPath.numOfRecords(samplingRate), tfs).
          asInstanceOf[HashMap[BlockId, java.lang.Long]]
        for ((blockId, count) <- numOfRecords) {
          m.put(blockId, count)
        }
        resultMap.put(samplingRate, m)
      }
    }
    resultMap
  }

  def getBlockSelectivity(): java.util.HashMap[java.lang.Double, Selectivity] = {
//    val blockSelectivity = new LinkedHashMap[Int, LinkedHashMap[BlockId, (Long, Long)]]
    val resultMap = new java.util.HashMap[java.lang.Double, Selectivity]
    for (samplingRate <- samplingRates) {
      if (Utils.tachyonFileExist(TachyonPath.blockSelectivity(samplingRate), tfs)) {
        val m = new Selectivity
        val blockSelectivity =
          Utils.readFromTachyonFile(TachyonPath.blockSelectivity(samplingRate), tfs).
          asInstanceOf[LinkedHashMap[Int, LinkedHashMap[BlockId, (Long, Long)]]]
        for ((stageId, stats) <- blockSelectivity) {
          m.put(stageId, new java.util.LinkedHashMap[BlockId, (java.lang.Long, java.lang.Long)])
          for ((blockId, pair) <- stats) {
            m.get(stageId).put(blockId, (new java.lang.Long(pair._1), new java.lang.Long(pair._2)))
          }
        }
        resultMap.put(samplingRate, m)
      }
    }
    resultMap
  }

  def getBlockGroups(samplingRate: Double): Map[Int, Set[BlockId]] = {
    val numOfRecords = Utils.readFromTachyonFile(TachyonPath.numOfRecords(samplingRate), tfs).
      asInstanceOf[HashMap[BlockId, Long]]
    val blockIds = numOfRecords.keySet
    val blockGroups = blockIds.groupBy(k => k.asRDDId.get.rddId)
    blockGroups
  }

//  def getSelectivityInputs: = {
//    for (samplingRate <- samplingRates) {
//      if (Utils.tachyonFileExist(TachyonPath.blockSelectivity(samplingRate), tfs)) {
//
//      }
//    }
//  }
  def getData(): HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]] = {
    val data = new HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]
    val paths = TachyonPath.trainingData()
    for (path <- paths) {
      val file = tfs.openIfExists(new TachyonURI(path))
      if (file != null) {
        val fos: FileInStream = new FileInStream(tfs.getInfo(file, GetInfoOptions.defaults()),
          InStreamOptions.defaults())
        val oos = new ObjectInputStream(fos)
        val obj: HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]] =
          oos.readObject().
            asInstanceOf[HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]]
        for ((storageLevel, map) <- obj) {
          for ((blockId, set) <- map) {
            data.getOrElseUpdate(storageLevel,
              new HashMap[BlockId, HashSet[(Double, BlockStatus)]]).
              getOrElseUpdate(blockId, new HashSet[(Double, BlockStatus)]) ++= set
          }
        }
        oos.close()
        fos.close()
      }
    }
    MyLog.info("Data: " + data.toString())
    data
  }

  def getInputs(): java.util.HashMap[BlockId,
    java.util.HashMap[String, (java.lang.Double, java.lang.Long, java.lang.Long)]] = {
    val data: HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]] = getData()
//    val inputs: HashMap[String, ]
    val res = new java.util.HashMap[BlockId,
      java.util.HashMap[String, (java.lang.Double, java.lang.Long, java.lang.Long)]]()
    for((storageLevel, map) <- data) {
      for ((blockId, points) <- map) {
        res.put(blockId, new java.util.HashMap[String, (java.lang.Double, java.lang.Long, java.lang.Long)])

        val (samplingRate, blockStatus) = points.head
        val numOfRecords = Utils.readFromTachyonFile(TachyonPath.numOfRecords(samplingRate), tfs).
          asInstanceOf[HashMap[BlockId, Long]]
        val num = numOfRecords.get(blockId).get

        if (StorageLevel.fromString(storageLevel).equals(StorageLevel.MEMORY_ONLY)) {
          for (field: FieldName.Value <- FieldName.values) {
            field match {
              case FieldName.MEM_SIZE => {
                res.get(blockId).put(field.toString, (samplingRate, num, blockStatus.memSize))
              }
              case FieldName.CPU_TIME | FieldName.COMPUTE_TIME => {
                res.get(blockId).put(field.toString, (samplingRate, num, blockStatus.avgComputeTime))
              }
              case _ => {}
            }
          }

        } else if (StorageLevel.fromString(storageLevel).equals(StorageLevel.DISK_ONLY)) {
          for (field: FieldName.Value <- FieldName.values) {
            field match {
              case FieldName.DISK_SIZE | FieldName.SER_TIME |
                   FieldName.DESER_TIME => {

              }
              case _ => {}
            }
          }
        }
      }
    }
    res
  }

  def main(args: Array[String]): Unit = {
    val blockGroups = getBlockGroups(0.2)
//    for
    getInputs()
  }
}
