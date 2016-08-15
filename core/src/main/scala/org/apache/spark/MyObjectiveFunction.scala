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

import org.apache.spark.storage.{BlockId, StorageLevel}
import org.coinor.opents._
import tachyon.client.file.TachyonFileSystem
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory

import scala.collection.mutable.{HashMap, HashSet, PriorityQueue}

class MyObjectiveFunction extends ObjectiveFunction with Logging {
  type PartitionDependency = HashMap[BlockId, HashSet[Int]] // blockId -> Set(stageId)

  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
  val candidateRDDs: List[BlockId] = List[BlockId]()
  // storageLevel -> (BlockId -> BlockStatus)
  //  val blockStats: HashMap[String, HashMap[BlockId, HashSet[BlockStatus]]] =
  //    new HashMap[String, HashMap[BlockId, HashSet[BlockStatus]]]()
  val blockStats: HashMap[BlockId, BlockStats] = new HashMap[BlockId, BlockStats]()
  val partitionDependency: PartitionDependency = new PartitionDependency()
  val stageMemory: HashMap[Int, Long] = new HashMap[Int, Long]
  // have order
  val stageBlocks: HashMap[Int, List[BlockId]] = new HashMap[Int, List[BlockId]]

  def this(executorId: String) = {
    this()

  }

  override def evaluate(solution: Solution, move: Move): Array[Double] = {
    val storageLevels = solution.asInstanceOf[MySolution].storageLevels
    val executorMemory = solution.asInstanceOf[MySolution].executorMemory
    val isMem = new HashMap[BlockId, Boolean]
    val isDisk = new HashMap[BlockId, Boolean]

    case class Elem(var priority: Int, blockId: BlockId)
    def MyOrdering = new Ordering[Elem] {
      def compare(a: Elem, b: Elem) = a.priority.compare(b.priority) * (-1)
    }

    if (move == null) {
      val time: Double = 0.0
      return Array[Double] {
        time
      }
    } else {
      // Else calculate incrementally
      var time: Double = 0.0
      val mv = move.asInstanceOf[MyMove]
      val blockId = mv.blockId
      val fromLevel: StorageLevel = storageLevels(blockId)
      val toLevel: StorageLevel = mv.toLevel

      val stageIds = partitionDependency(blockId)
      val startStageId = stageIds.min
      val endStageId = stageBlocks.keySet.max

      val pq = new PriorityQueue[Elem]()(MyOrdering)

      for (id <- 0 to startStageId - 1) {
        // have order
        val blockIdsThisStage = stageBlocks.getOrElse(id, List.empty)
        for (bid <- blockIdsThisStage) {
          MyLog.info("Stage: " + id + " " + "Block: " + bid)
          pq.find(x => x.blockId.equals(bid)) match {
            case Some(elem: Elem) => elem.priority += 1
            case None => pq.enqueue(new Elem(1, bid))
          }

          val stats = blockStats(bid)
          if (storageLevels(bid).useMemory) {
            val blockSize = if (storageLevels(bid).deserialized) stats.stats(FieldName.MEM_SIZE)
              else stats.stats(FieldName.DISK_SIZE)
            val blockTime = if (storageLevels(bid).deserialized) stats.stats(FieldName.COMPUTE_TIME)
              else stats.stats(FieldName.COMPUTE_TIME) + stats.stats(FieldName.SER_TIME)
            val otherBlocks = pq.filter((elem: Elem) =>
              (elem.blockId.asRDDId.get.rddId != bid.asRDDId.get.rddId))
            while (otherBlocks.size > 0 && executorMemory < blockSize) {

            }
          } else {

          }
        }
      }

      for (id <- startStageId to endStageId) {
        // have order
        val blockIdsThisStage = stageBlocks.getOrElse(id, List.empty)
        for (bid <- blockIdsThisStage) {
          if (bid.equals(blockId)) {

          } else {

          }
        }
      }
    }
    return null
  }

  def setCandidateRDDs(): Unit = {

  }

  def setBlockStats(): Unit = {

  }

  def setPartitionDependency(): Unit = {

  }
}
