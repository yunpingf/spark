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
import org.apache.spark.storage.{StorageLevel, BlockId, BlockStatus}
import org.apache.spark.util.Utils
import tachyon.TachyonURI
import tachyon.client.file.options.{InStreamOptions, GetInfoOptions}
import tachyon.client.file.{FileInStream, TachyonFileSystem}
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory

import scala.collection.mutable.{HashMap, HashSet}

// scalastyle:off println
object TabuSearch {
  val predefinedRates = List(0.1, 0.2, 0.3)
  val predefinedStorageLevel = List(StorageLevel.MEMORY_ONLY.toString,
    StorageLevel.MEMORY_ONLY_SER.toString)
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()

  def search(): Unit = {
    val taskDescs = Utils.readFromTachyonFile(TachyonPath.taskDescription, tfs).
      asInstanceOf[Seq[Seq[TaskDescription]]]
    val numOfExecutors = taskDescs.length

  }

  def parsePoints(pointsSet: HashSet[(Double, BlockStatus)], field: String):
  (List[Double], HashMap[FieldName.Value, List[Double]]) = {
    val points = pointsSet.toList
    val x = points.map(v => v._1)

    val res = new HashMap[FieldName.Value, List[Double]]()
    for (field: FieldName.Value <- FieldName.values){
      val y = field match {
        case FieldName.MEM_SIZE => points.map(v => v._2.memSize)
        case FieldName.DISK_SIZE => points.map(v => v._2.diskSize)
        case FieldName.SER_TIME => points.map(v => v._2.avgSerializeTime)
        case FieldName.DESER_TIME => points.map(v => v._2.avgDeserializeTime)
        case FieldName.CPU_TIME => points.map(v => v._2.avgCpuTime)
        case FieldName.COMPUTE_TIME => points.map(v => v._2.avgComputeTime)
      }
    }
    (x, res)
  }

  def linearRegression(): Unit = {
    val data = new HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]
    val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
    val paths = Utils.getTachyonPaths()
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

    def predict(x: List[Double], y: HashMap[FieldName.Value, List[Double]],
                fieldName: FieldName.Value): Long = {
      val regression = new SimpleRegression()
      val ys = y(fieldName)
      val len = x.size
      for (i <- 0 to len - 1) {
        regression.addData(x(i), ys(i))
      }
      regression.predict(1.0).toLong
    }

    val res = new HashMap[BlockId, BlockStats]()
    for ((storageLevel, map) <- data) { // for each storageLevel
      for ((blockId, points) <- map) { // for each blockId, there are multiple samplings
        val parsedPoints = parsePoints(points, "")
        val regression = new SimpleRegression()
        // 0.1, 0.2, 0.3
        val x: List[Double] = parsedPoints._1
        // fieldName -> (0.1, 0.2, 0.3)
        val y: HashMap[FieldName.Value, List[Double]] = parsedPoints._2


        val stats: BlockStats = res.getOrElseUpdate(blockId, new BlockStats())

        if (storageLevel.equals(StorageLevel.MEMORY_ONLY)) {
          for (field: FieldName.Value <- FieldName.values) {
            field match {
              case FieldName.MEM_SIZE => {
                stats.stats.put(field, predict(x, y, field))
              }
              case FieldName.CPU_TIME | FieldName.COMPUTE_TIME => {
                if (stats.stats.contains(field)){
                  val t = predict(x, y, field) + stats.stats(field)
                  stats.stats.update(field, t / 2)
                } else {
                  stats.stats.put(field, predict(x, y, field))
                }
              }
              case _ => {}
            }
          }
        } else if (storageLevel.equals(StorageLevel.MEMORY_ONLY_SER)) {
          for (field: FieldName.Value <- FieldName.values) {
            field match {
              case FieldName.DISK_SIZE | FieldName.SER_TIME |
                   FieldName.DESER_TIME => {
                stats.stats.put(field, predict(x, y, field))
              }
              case FieldName.CPU_TIME | FieldName.COMPUTE_TIME => {
                if (stats.stats.contains(field)){
                  val t = predict(x, y, field) + stats.stats(field)
                  stats.stats.update(field, t / 2)
                } else {
                  stats.stats.put(field, predict(x, y, field))
                }
              }
              case _ => {}
            }
          }
        }
        // val model = regression.ols(parsedPoints._1, parsedPoints._2)
        // println(model.predict(Array(1.0)))
      }
    }
    MyLog.info("Prediction Finished")
    for ((blockId, blockStats) <- res) {
      MyLog.info(blockId + " " + blockStats.toString)
    }
  }

  def cleanFS(): Unit = {
    val paths = TachyonPath.taskDescription :: TachyonPath.rddDependency :: Utils.getTachyonPaths()
    for (p <- paths) {
      Utils.deleteTachyonFile(p, tfs)
    }
  }

  def main(args: Array[String]): Unit = {
    this.linearRegression()
  }
}

// scalastyle:on println