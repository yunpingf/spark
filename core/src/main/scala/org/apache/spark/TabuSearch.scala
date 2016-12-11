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

import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.storage.{BlockId, BlockStatus, StorageLevel}
import org.apache.spark.util.Utils
import tachyon.TachyonURI
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory
import tachyon.client.file.options.{GetInfoOptions, InStreamOptions}
import tachyon.client.file.{FileInStream, TachyonFileSystem}

import scala.collection.mutable.{HashMap, HashSet}

// scalastyle:off println
object TabuSearch {
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()

  def search(): Unit = {
    val taskDescs = Utils.readFromTachyonFile(TachyonPath.taskDescription, tfs).
      asInstanceOf[Seq[Seq[TaskDescription]]]
    val numOfExecutors = taskDescs.length

  }

//  def parsePoints(pointsSet: HashSet[(Double, BlockStatus)], storageLevel: StorageLevel):
//  (List[Double], HashMap[FieldName.Value, List[Double]]) = {
//    val points = pointsSet.toList
//    val x = points.map(v => v._1)
//
//    val res = new HashMap[FieldName.Value, List[Double]]()
//    for (field: FieldName.Value <- FieldName.values) {
//      val y = field match {
//        case FieldName.MEM_SIZE => {
//          if (storageLevel.equals(StorageLevel.MEMORY_ONLY)) {
//            points.map(v => v._2.memSize.toDouble)
//          } else {
//            List.empty[Double]
//          }
//        }
//        case FieldName.DISK_SIZE => {
//          if (storageLevel.equals(StorageLevel.MEMORY_ONLY_SER)) {
//            points.map(v => v._2.memSize.toDouble)
//          } else {
//            List.empty[Double]
//          }
//        }
//        case FieldName.SER_TIME => points.map(v => v._2.avgSerializeTime.toDouble)
//        case FieldName.DESER_TIME => points.map(v => v._2.avgDeserializeTime.toDouble)
//        case FieldName.CPU_TIME => points.map(v => v._2.avgCpuTime.toDouble)
//        case FieldName.COMPUTE_TIME => points.map(v => v._2.avgComputeTime.toDouble)
//      }
//      res.put(field, res.getOrElse(field, List[Double]()) ++ y)
//    }
//    (x, res)
//  }

  def linearRegression(): Unit = {
    val data = new HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]
    val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
    val paths = TachyonPath.trainingData()
    System.out.println("Paths: " + paths)
    for (path <- paths) {
      val file = tfs.openIfExists(new TachyonURI(path))
      if (file != null) {
        val fos: FileInStream = new FileInStream(tfs.getInfo(file, GetInfoOptions.defaults()),
          InStreamOptions.defaults())
        val oos = new ObjectInputStream(fos)
        val obj: HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]] =
          oos.readObject().
            asInstanceOf[HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]]
        MyLog.info(obj.toString())
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
    val res = new HashMap[BlockId, BlockStats]()
    val cacheDiskSize = new HashMap[BlockId, Long]()
    val cacheMemorySize = new HashMap[BlockId, Long]()
    for ((storageLevel, map) <- data){
      println(map)
      for ((blockId, set) <- map) {
        for ((samplingRate, blockStatus) <- set) {
          if (samplingRate != 1.0) {
            if (StorageLevel.fromString(storageLevel).useDisk) {
              cacheDiskSize.put(blockId, blockStatus.diskSize)
            } else {
              cacheMemorySize.put(blockId, blockStatus.memSize)
            }
          }
        }
      }
    }
    println(cacheDiskSize)
    println(cacheMemorySize)

    for ((storageLevel, map) <- data){
      for ((blockId, set) <- map) {
        for ((samplingRate, blockStatus) <- set) {
          if (samplingRate != 1.0){

          } else {
            if (StorageLevel.fromString(storageLevel).useMemory){
              val blockStats = new BlockStats()
              val rate = cacheMemorySize.get(blockId).get * 1.0/  cacheDiskSize.get(blockId).get
              System.out.println(rate)
              blockStats.stats.put(FieldName.COMPUTE_TIME, blockStatus.avgComputeTime)
              blockStats.stats.put(FieldName.CPU_TIME, blockStatus.avgCpuTime)
              blockStats.stats.put(FieldName.MEM_SIZE, (blockStatus.memSize * rate).toLong)
              blockStats.stats.put(FieldName.DISK_SIZE, blockStatus.memSize)
              blockStats.stats.put(FieldName.SER_TIME, blockStatus.avgSerializeTime)
              blockStats.stats.put(FieldName.DESER_TIME, blockStatus.avgDeserializeTime)
              res.put(blockId, blockStats)
            }
          }
        }
      }
    }
//    def predict(x: List[Double], y: HashMap[FieldName.Value, List[Double]],
//                fieldName: FieldName.Value): Long = {
//      // val model = regression.ols(parsedPoints._1, parsedPoints._2)
//      // println(model.predict(Array(1.0)))
//      val regression = new SimpleRegression()
//      val ys = y(fieldName)
//      val len = x.size
//      for (i <- 0 to len - 1) {
//        regression.addData(x(i), ys(i))
//      }
//      val avgByMe = ys.sum * 5 / ys.size
//      MyLog.info("Average value for prediction: " + avgByMe)
//      val predictVal = regression.predict(1.0).toLong
//      MyLog.info("Regression value for prediction: " + predictVal)
//      if (predictVal < 0) {
//        avgByMe.toLong
//      } else {
//        predictVal
//      }
//    }
//
//    val res = new HashMap[BlockId, BlockStats]()
//    for ((storageLevel, map) <- data) {
//      // for each storageLevel
//      for ((blockId, points) <- map) {
//        // for each blockId, there are multiple samplings
//        val parsedPoints = parsePoints(points, StorageLevel.fromString(storageLevel))
//        // 0.1, 0.2, 0.3
//        val x: List[Double] = parsedPoints._1
//        // fieldName -> (0.1, 0.2, 0.3)
//        val y: HashMap[FieldName.Value, List[Double]] = parsedPoints._2
//
//
//        val stats: BlockStats = res.getOrElseUpdate(blockId, new BlockStats())
//
//        if (StorageLevel.fromString(storageLevel).equals(StorageLevel.MEMORY_ONLY)) {
//          for (field: FieldName.Value <- FieldName.values) {
//            field match {
//              case FieldName.MEM_SIZE => {
//                stats.stats.put(field, predict(x, y, field))
//              }
//              case FieldName.CPU_TIME | FieldName.COMPUTE_TIME => {
//                if (stats.stats.contains(field)) {
//                  val t = predict(x, y, field) + stats.stats(field)
//                  stats.stats.update(field, t / 2)
//                } else {
//                  stats.stats.put(field, predict(x, y, field))
//                }
//              }
//              case _ => {}
//            }
//          }
//        } else if (StorageLevel.fromString(storageLevel).equals(StorageLevel.MEMORY_ONLY_SER)) {
//          for (field: FieldName.Value <- FieldName.values) {
//            field match {
//              case FieldName.DISK_SIZE | FieldName.SER_TIME |
//                   FieldName.DESER_TIME => {
//                stats.stats.put(field, predict(x, y, field))
//              }
//              case FieldName.CPU_TIME | FieldName.COMPUTE_TIME => {
//                if (stats.stats.contains(field)) {
//                  val t = predict(x, y, field) + stats.stats(field)
//                  stats.stats.update(field, t / 2)
//                } else {
//                  stats.stats.put(field, predict(x, y, field))
//                }
//              }
//              case _ => {}
//            }
//          }
//        }
//        // val model = regression.ols(parsedPoints._1, parsedPoints._2)
//        // println(model.predict(Array(1.0)))
//      }
//    }
    MyLog.info("Prediction unified result: " + res.toString())

    Utils.writeToTachyonFile(TachyonPath.rddPrediction, res, tfs, true)
    val resRead = Utils.readFromTachyonFile(TachyonPath.rddPrediction, tfs).
      asInstanceOf[HashMap[BlockId, BlockStats]]
    for ((blockId, blockStats) <- resRead) {
      MyLog.info(blockId + " " + blockStats.toString)
    }
  }

  def cleanFS(): Unit = {
    val paths = TachyonPath.taskDescription :: TachyonPath.rddDependency ::
      TachyonPath.rddPrediction :: TachyonPath.executorIdToParDep :: TachyonPath.trainingData()
    for (p <- paths) {
      Utils.deleteTachyonFile(p, tfs)
    }
  }

  def main(args: Array[String]): Unit = {
    this.linearRegression()
//    this.cleanFS()
  }
}

// scalastyle:on println