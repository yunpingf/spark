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
import org.apache.spark.storage.BlockManagerMessages.{GetBlockStatus, HelloMaster}
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, ThreadUtils, SizeEstimator, Utils}
import tachyon.TachyonURI
import tachyon.client.file.options.{GetInfoOptions, InStreamOptions, OutStreamOptions}
import tachyon.client.file.{FileInStream, TachyonFile, FileOutStream, TachyonFileSystem}
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, LinkedHashMap}
import scala.collection.{Iterable, immutable}
import scala.concurrent.Future

// scalastyle:off println
class StatsCollectorListener(val sc: SparkContext, val blockManagerMasterEndpoint: RpcEndpointRef)
  extends SparkListener with Logging {
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
  var runMode: String = RunMode.FULL
  var samplingRate: Double = 1.0
  var storageLevel: String = StorageLevel.MEMORY_ONLY.toString
  val blockStats = new HashMap[String, HashMap[BlockId, HashSet[(Double, BlockStatus)]]]
  val taskScheduler: TaskSchedulerImpl = sc.getTaskScheduler()
  var stageRdds: HashMap[Int, RDD[_]] = new HashMap[Int, RDD[_]]
  val candidateRDDs = new HashSet[RDD[_]]
  val rddToChildren = new HashMap[RDD[_], HashSet[RDD[_]]]
  val notPersistRDDs = new HashSet[Int]

  private def setTrainingStorageLevel(): Unit = {

  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    MyLog.info("The application is about to end")
    for (rdd <- candidateRDDs) {
      MyLog.info("Rdd: " + rdd)
      val partitions = rdd.partitions
      for (p <- partitions) {
        MyLog.info("Partitions: " + p)
        val blockId = new RDDBlockId(rdd.id, p.index)
        val msg = GetBlockStatus(blockId, true)
        val response = blockManagerMasterEndpoint.
          askWithRetry[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
        val (blockManagerIds, futures) = response.unzip
        MyLog.info(blockManagerIds.toString())
        MyLog.info(futures.toString())
        implicit val sameThread = ThreadUtils.sameThread
        val cbf =
          implicitly[
            CanBuildFrom[Iterable[Future[Option[BlockStatus]]],
              Option[BlockStatus],
              Iterable[Option[BlockStatus]]]]
        val timeout = RpcUtils.askRpcTimeout(sc.conf)
        MyLog.info("!!!!!!!!!$$$$$")
        val blockStatus = timeout.awaitResult(
          Future.sequence[Option[BlockStatus], Iterable](futures)(cbf, ThreadUtils.sameThread))
        MyLog.info("BlockId= " + blockId + " " + blockStatus.toString())
        if (blockStatus == null) {
          throw new SparkException("BlockManager returned null for BlockStatus query: " + blockId)
        }
        MyLog.info("No Exception")
        val status = blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
          status.map { s => (blockManagerId, s) }
        }.toMap
        MyLog.info("Status: " + status)
        for ((blockManagerId, blockStatus) <- status) {
          MyLog.info("BlockManagerId: " + blockManagerId)
          MyLog.info("BlockStatus: " + blockStatus)
        }
      }
    }



    
    if (runMode == RunMode.TRAINING && !Utils.tachyonFileExist(TachyonPath.candidateRdds, tfs)){
      MyLog.info("About to write dep")
      println("About to write dep")
      Utils.writeToTachyonFile(getPath(), blockStats, tfs, true)

      val executorIdToTasks = taskScheduler.getExecutorIdToTasks()
      // executorId -> Array(stageId, partitionId)
      MyLog.info("ExecutorId To Tasks:"  + executorIdToTasks.toString())

      val stageIds = stageRdds.keySet.toList.sorted
      // blockId -> Array(stageId)
      // Use LinkedHashMap to keep order
      type PartitionDependency = LinkedHashMap[BlockId, ArrayBuffer[Int]]
      val executorIdToParDep = new LinkedHashMap[String, PartitionDependency]

      def findDps(rdd: RDD[_], partitions: Seq[Int],
                  stageId: Int, dp: PartitionDependency,
                  previousBlockIds: ArrayBuffer[BlockId]): Unit = {
        if (candidateRDDs.contains(rdd)) {
          for (p <- partitions) {
            val newBlockId = new RDDBlockId(rdd.id, p)
            if (previousBlockIds.contains(newBlockId)) {
              dp.getOrElseUpdate(newBlockId, new ArrayBuffer[Int]).append(stageId)
            } else {
              previousBlockIds.append(newBlockId)
            }
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

      if (Utils.tachyonFileExist(TachyonPath.executorIdToParDep, tfs)) {
        executorIdToParDep.++=(
          Utils.readFromTachyonFile(TachyonPath.executorIdToParDep, tfs).
            asInstanceOf[LinkedHashMap[String, PartitionDependency]])
      } else {
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
      }

      Utils.writeToTachyonFile(TachyonPath.executorIdToParDep, executorIdToParDep, tfs, true)
      MyLog.info("Stage RDDs: " + stageRdds.map(p => (p._1, p._2.id)))
      MyLog.info("Candidate RDDs: " + candidateRDDs.map(p => p.id))
      MyLog.info("ExecutorIdToParDep: " + executorIdToParDep.toString())


      MyLog.info("Block Stats: " + blockStats.toString())

      val filteredRddDep = rddToChildren.filter((p: (RDD[_], HashSet[RDD[_]])) => p._2.size > 1)
      MyLog.info("rddToChildren: " + rddToChildren.map((p: (RDD[_], HashSet[RDD[_]])) =>
        (p._1.id, p._2.map(x => x.id))))
      MyLog.info("rddToChildren Size: " + rddToChildren.size)

      MyLog.info("filteredDep: " + filteredRddDep.map((p: (RDD[_], HashSet[RDD[_]])) =>
        (p._1.id, p._2.map(x => x.id))))

      Utils.writeToTachyonFile(
        TachyonPath.candidateRdds, candidateRDDs.map(p => p.id), tfs, true)

//      MyLog.info("Broadcast Block")
//      MyLog.info(Utils.readFromTachyonFile(TachyonPath.broadcastBlock, tfs).toString)
    }
    else if (runMode == RunMode.FULL){
      MyLog.info("Run Mode: " + runMode)
      val executorIdToTasks = taskScheduler.getExecutorIdToTasks()
      MyLog.info(executorIdToTasks.toString())
    } else {
      MyLog.info("Run Mode: " + runMode + " No ExectuorId to Dep")
      Utils.writeToTachyonFile(getPath(), blockStats, tfs, true)
      MyLog.info("Block Stats: " + blockStats.toString())
    }

  }

  override def onBuildRddDependency(buildRddDependency: SparkListenerBuildRddDependency): Unit = {
    MyLog.info("Building Dependency")
    runMode = buildRddDependency.runMode
    samplingRate = buildRddDependency.samplingRate.toDouble
    storageLevel = buildRddDependency.storageLevel


    def buildDependency(rdd: RDD[_]): Unit = {
      val parentRDDs = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]]).map(_.rdd)
      for (pr <- parentRDDs) {
        if (rddToChildren.contains(pr)) {
          rddToChildren.get(pr).get.add(rdd)
          if (runMode == RunMode.TRAINING &&
            rddToChildren.get(pr).get.size > 1 && !candidateRDDs.contains(pr)) {
            MyLog.info("RDD " + pr.id + " persist 2 " + storageLevel)
            pr.persist(StorageLevel.fromString(storageLevel), true)
            candidateRDDs.add(pr)
          }
        } else {
          rddToChildren.put(pr, new HashSet[RDD[_]])
          rddToChildren.get(pr).get.add(rdd)
        }
        buildDependency(pr)
      }
    }

    def buildDependency2(rdd: RDD[_], parentIds: HashSet[Int]): Unit = {
      val parentRDDs = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]]).map(_.rdd)
      for (pr <- parentRDDs) {
        if (parentIds.contains(pr.id)) {
          MyLog.info("RDD " + pr.id + " persist to " + storageLevel)
          pr.persist(StorageLevel.fromString(storageLevel), true)
        }
        buildDependency2(pr, parentIds)
      }
    }


    if (runMode == RunMode.TRAINING) {
      if (Utils.tachyonFileExist(TachyonPath.candidateRdds, tfs)) {
        val parentIds =
          Utils.readFromTachyonFile(TachyonPath.candidateRdds, tfs).
            asInstanceOf[HashSet[Int]]
        MyLog.info("Read Candidate RDDs from Tachyon: " + parentIds)
        for ((id, rdd) <- buildRddDependency.stageRdds) {
          buildDependency2(rdd, parentIds)
        }
      } else {
        MyLog.info("Build from scratch")
        for ((id, rdd) <- buildRddDependency.stageRdds) {
          buildDependency(rdd)
        }
      }
      stageRdds ++= buildRddDependency.stageRdds// stageId -> RDD
    } else {
      val rddResult = Utils.readFromTachyonFile(TachyonPath.rddResult, tfs).
        asInstanceOf[HashMap[BlockId, StorageLevel]]
      MyLog.info("RDD Result: " + rddResult)

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
      if (Utils.tachyonFileExist(TachyonPath.candidateRdds, tfs)) {
        val parentIds =
          Utils.readFromTachyonFile(TachyonPath.candidateRdds, tfs).
            asInstanceOf[HashSet[Int]]
        for ((blockId, blockStatus) <- data) {
          if (blockId.isRDD && parentIds.contains(blockId.asRDDId.get.rddId)) {
            map.getOrElseUpdate(blockId, new HashSet[(Double, BlockStatus)]).
              add((samplingRate, blockStatus))
          }
        }
      } else {
        map.getOrElseUpdate(blockId, new HashSet[(Double, BlockStatus)]).
          add((samplingRate, blockStatus))
        println("BlockId: " + blockId)
        println("!!!!!" + map.get(blockId).size)
      }

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
//    if (!metrics.isEmpty) {
//      MyLog.info("TaskEnd Block Status: " + taskEnd.taskMetrics.blockStatus)
//      buildBlockStats(taskEnd.taskMetrics.blockStatus)
//    }

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
    }

  }
  // scalastyle:on println
}
