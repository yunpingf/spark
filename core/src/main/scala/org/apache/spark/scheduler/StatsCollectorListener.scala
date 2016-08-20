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
package org.apache.spark.scheduler

import java.io.{ObjectInputStream, ObjectOutputStream, FileInputStream}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerMessages.HelloMaster
import org.apache.spark.storage.{RDDBlockId, BlockId, StorageLevel, BlockStatus}
import org.apache.spark.util.Utils
import tachyon.TachyonURI
import tachyon.client.file.options.{GetInfoOptions, InStreamOptions, OutStreamOptions}
import tachyon.client.file.{FileInStream, TachyonFile, FileOutStream, TachyonFileSystem}
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, LinkedHashMap}

// scalastyle:off println
class StatsCollectorListener(val sc: SparkContext, val blockManagerMasterEndpoint: RpcEndpointRef)
  extends SparkListener with Logging {
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
  var runMode: String = _
  var samplingRate: Double = _
  var storageLevel: String = _
  val blockStats = new HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]
  val taskScheduler: TaskSchedulerImpl = sc.getTaskScheduler()
  var stageRdds: HashMap[Int, RDD[_]] = new HashMap[Int, RDD[_]]
  val candidateRDDs = new ArrayBuffer[RDD[_]]
  val rddToChildren = new HashMap[RDD[_], HashSet[RDD[_]]]

  private def setTrainingStorageLevel(): Unit = {

  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

  }

  private def readFromFile(): Unit = {
    val path = new TachyonURI(getPath())
    var file: TachyonFile = tfs.open(path)
    val fos: FileInStream = new FileInStream(tfs.getInfo(file, GetInfoOptions.defaults()),
      InStreamOptions.defaults())
    // outputStream.write(string.getBytes(Charset.forName("UTF-8")));
    val oos = new ObjectInputStream(fos)
    println(oos.readObject())
    oos.close()
    fos.close()
  }

  private def writeToFile(): Unit = {
    val path = new TachyonURI(getPath())
    var file: TachyonFile = tfs.openIfExists(path)
    if (file != null) {
      tfs.delete(file)
    }
    file = tfs.create(path)
    val fos: FileOutStream = new FileOutStream(file.getFileId, OutStreamOptions.defaults())
    // outputStream.write(string.getBytes(Charset.forName("UTF-8")));
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(blockStats)
    oos.close()
    fos.close()
  }


  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (runMode == RunMode.TRAINING){
      writeToFile()
      readFromFile()

      val executorIdToTasks = taskScheduler.getExecutorIdToTasks()
      // executorId -> Array(stageId, partitionId)
      MyLog.info(executorIdToTasks.toString())

      val stageIds = stageRdds.keySet.toList.sorted
      // blockId -> Array(stageId)
      // Use LinkedHashMap to keep order
      type PartitionDependency = LinkedHashMap[BlockId, ArrayBuffer[Int]]
      val executorIdToParDep = new LinkedHashMap[String, PartitionDependency]

      def findDps(rdd: RDD[_], partitions: Seq[Int],
                  stageId: Int, dp: PartitionDependency,
                  previousBlockIds: ArrayBuffer[BlockId]): Unit = {
        for (p <- partitions) {
          val newBlockId = new RDDBlockId(rdd.id, p)
          if (previousBlockIds.contains(newBlockId)) {
            dp.getOrElseUpdate(newBlockId, new ArrayBuffer[Int]).append(stageId)
          } else {
            previousBlockIds.append(newBlockId)
          }
        }

        val parentDps = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]]).
          map(x => x.asInstanceOf[NarrowDependency[_]])
        for (parentDp <- parentDps) {
          val parentRDD = parentDp.rdd
          for (p <- partitions) {
            val parentPartitions = parentDp.getParents(p)
            findDps(parentRDD, parentPartitions, stageId, dp, previousBlockIds)
          }
        }
      }

      for ((executorId, tasks) <- executorIdToTasks) {
        val dp = new PartitionDependency
        val previousBlockIds = new ArrayBuffer[BlockId]
        for (stageId <- stageIds) {
          val stageRDD = stageRdds(stageId)
          val stageTasks = tasks.filter(t => t.stageId == stageId)
          val stagePartitions = stageTasks.map(t => t.partitionId)
          findDps(stageRDD, stagePartitions, stageId, dp, previousBlockIds)
        }

        executorIdToParDep.put(executorId,
          dp.transform((k, v) => v.distinct).retain((k, v) => v.size > 1))
      }

      Utils.writeToTachyonFile(TachyonPath.executorIdToParDep, executorIdToParDep, tfs, true)
      println("ExecutorIdToParDep: " + executorIdToParDep.toString())
      MyLog.info("ExecutorIdToParDep: " + executorIdToParDep.toString())
      MyLog.info(Utils.readFromTachyonFile(TachyonPath.executorIdToParDep, tfs).toString)
      MyLog.info(blockStats.toString())
    }
    else {
      MyLog.info("Run Mode: " + runMode)
      val executorIdToTasks = taskScheduler.getExecutorIdToTasks()
      MyLog.info(executorIdToTasks.toString())
    }

  }

  override def onBuildRddDependency(buildRddDependency: SparkListenerBuildRddDependency): Unit = {
    // rddDependency = Utils.readFromTachyonFile(TachyonPath.rddDependency, tfs).
    //  asInstanceOf[HashMap[Int, HashSet[Int]]]
    MyLog.info("Building Dependency")
    runMode = buildRddDependency.runMode
    samplingRate = buildRddDependency.samplingRate.toDouble
    storageLevel = buildRddDependency.storageLevel
    stageRdds ++= buildRddDependency.stageRdds// stageId -> RDD

    def buildDependency(rdd: RDD[_]): Unit = {
      val parentRDDs = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]]).map(_.rdd)
      for (pr <- parentRDDs) {
        if (rddToChildren.contains(pr)) {
          rddToChildren.get(pr).get.add(rdd)
          if (runMode == RunMode.TRAINING) {
            pr.persist(StorageLevel.fromString(storageLevel), true)
            candidateRDDs.append(pr)
          }
        } else {
          rddToChildren.put(pr, new HashSet[RDD[_]])
        }
        buildDependency(pr)
      }
    }


    if (runMode == RunMode.TRAINING) {
      for ((id, rdd) <- stageRdds) {
        buildDependency(rdd)
      }
    } else {
      val rddResult = Utils.readFromTachyonFile(TachyonPath.rddResult, tfs).
        asInstanceOf[HashMap[BlockId, StorageLevel]]
      MyLog.info("RDD Result: " + rddResult)
      println("RDD Result: " + rddResult)

    }

    blockManagerMasterEndpoint.askWithRetry[Boolean](HelloMaster("I'm Baymax"))
  }

  private def getPath(): String = {
    return "/" + (System.getProperty("spark.app.name").split('.').last) + "_" +
      storageLevel + "_" + samplingRate
  }

  private def buildBlockStats(data: HashMap[BlockId, BlockStatus]): Unit = {
    // HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]
    val map = blockStats.getOrElseUpdate(storageLevel,
      new HashMap[BlockId, HashSet[(Double, BlockStatus)]])
    for ((blockId, blockStatus) <- data) {
      map.getOrElseUpdate(blockId, new HashSet[(Double, BlockStatus)]).
        add((samplingRate, blockStatus))
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val (errorMessage, metrics): (Option[String], Option[TaskMetrics]) =
      taskEnd.reason match {
        case org.apache.spark.Success =>
          (None, Option(taskEnd.taskMetrics))
        case e: ExceptionFailure => // Handle ExceptionFailure because we might have metrics
          (Some(e.toErrorString), e.metrics)
        case e: TaskFailedReason => // All other failure cases
          (Some(e.toErrorString), None)
      }
    if (!metrics.isEmpty) {
      buildBlockStats(taskEnd.taskMetrics.blockStatus)
    }

    if (!metrics.isEmpty) {
      val taskMetrics = taskEnd.taskMetrics
      val taskInfo = taskEnd.taskInfo
      MyLog.info("=====Task id: " + taskInfo.taskId + " " +
        "Executor id: " + taskInfo.executorId + "=====")
      val xxx = taskMetrics.rddIdToComputeTime;
      for ((k, v) <- xxx) {
        val rddId = k.asRDDId.get
        for (yyy <- v) {
          MyLog.info(rddId.rddId + " " + rddId.splitIndex + "(" + yyy._1 + " " + yyy._2 + ")")
        }
      }
      taskMetrics.inputMetrics match {
        case Some(inputMetrics) => {
          MyLog.info("Input Bytes Read: " + inputMetrics.bytesRead + " " +
            "Input Records Read: " + inputMetrics.recordsRead)
        }
        case None => {

        }
      }
      taskMetrics.outputMetrics match {
        case Some(outputMetrics) => {
          MyLog.info("Output Bytes Written: " + outputMetrics.bytesWritten +
            " " + "Output Records Written: " +  outputMetrics.recordsWritten)
        }
        case None => {

        }
      }
    }

  }
  // scalastyle:on println
}
