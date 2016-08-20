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

import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils
import org.coinor.opents._
import tachyon.client.file.TachyonFileSystem
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory

import scala.collection.mutable.{ArrayBuffer, HashMap, LinkedHashMap}

class MyObjectiveFunction(blockIdToStages: LinkedHashMap[BlockId, ArrayBuffer[Int]])
  extends ObjectiveFunction with Logging {
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
  val blockStats: HashMap[BlockId, BlockStats] = getBlockStats()
  // blocks in this stage have order
  val stageBlocks: HashMap[Int, List[BlockId]] = getStageBlocks()

  override def evaluate(solution: Solution, move: Move): Array[Double] = {
    if (move == null) {
      evaluateAbsolutely(solution, move)
    } else {
      logInfo("Evaluating this move: " + move)
      move.operateOn(solution)
      evaluateAbsolutely(solution, move)
    }
  }

  def evaluateAbsolutely(solution: Solution, move: Move):
  Array[Double] = {
    val storageLevels = solution.asInstanceOf[MySolution].storageLevels

    val startStageId = stageBlocks.keySet.min
    val endStageId = stageBlocks.keySet.max
    val isMem = new HashMap[BlockId, Boolean]
    val isDisk = new HashMap[BlockId, Boolean]
    var time: Long = 0
    var executorMemory: Long = solution.asInstanceOf[MySolution].executorMemory
    val entries = new ArrayBuffer[BlockId]()
    for (stageId <- startStageId to endStageId) {
      logInfo("Current Stage: " + stageId)
      // blocks in this stage, can be empty if there is no candidate blocks
      val blocks: List[BlockId] = stageBlocks.getOrElse(stageId, List.empty[BlockId])
      logInfo("Blocks in this stage: " + blocks)
      for (blockId <- blocks) {
        logInfo("Evaluating block: " + blockId)
        if (isMem.getOrElse(blockId, false)) {
          // read from memory
          logInfo("Read from memory")
        } else {
          if (isDisk.getOrElse(blockId, false)) {
            // read from disk
            logInfo("Read from disk")
            time += blockStats(blockId).stats(FieldName.DESER_TIME)
          } else {
            // compute from scratch
            logInfo("Compute from scratch")
            time += blockStats(blockId).stats(FieldName.COMPUTE_TIME)
          }
          // try to store
          if (storageLevels(blockId).useMemory) {
            val otherBlocks =
              entries.filter(x => (x.asRDDId.get.rddId == blockId.asRDDId.get.rddId))
            val selectedBlocks = new ArrayBuffer[BlockId]
            val iter = otherBlocks.iterator
            val blockSize = blockStats(blockId).stats(FieldName.MEM_SIZE)
            while (executorMemory < blockSize && iter.hasNext) {
              val nextBlockId = iter.next()
              selectedBlocks += nextBlockId
              executorMemory += blockStats(nextBlockId).stats(FieldName.MEM_SIZE)
              logInfo(nextBlockId + " is evicted from memory")
              isMem(nextBlockId) = false
            }
            if (executorMemory > blockSize) {
              isMem(blockId) = true
              entries.append(blockId)
              logInfo(blockId + " is put in memory")
            } else {
              logInfo(blockId + "can't be put in memory")
              if (storageLevels(blockId).useDisk) {
                isDisk(blockId) = true
                time += blockStats(blockId).stats(FieldName.SER_TIME)
              }
            }
            logInfo("Time: " + time)
            logInfo("Executor memory left: " + executorMemory)
            solution.asInstanceOf[MySolution].executorMemoryVar = executorMemory
          } else if (storageLevels(blockId).useDisk) {
            logInfo("Put " + blockId + " into disk")
            isDisk(blockId) = true
            time += blockStats(blockId).stats(FieldName.SER_TIME)
            logInfo("Time: " + time)
          }
        }
      }
    }
    return Array[Double] {
      time
    }
  }

  def evaluateIncrementally(solution: Solution, move: Move):
  Array[Double] = {
    move.operateOn(solution)
    evaluateAbsolutely(solution, move)
    return null
  }

  def setCandidateRDDs(): Unit = {

  }

  def getBlockStats(): HashMap[BlockId, BlockStats] = {
    // HashMap[BlockId, BlockStats]
    val stats = Utils.readFromTachyonFile(TachyonPath.rddPrediction, tfs).
      asInstanceOf[HashMap[BlockId, BlockStats]]
    stats
  }

  def getStageBlocks(): HashMap[Int, List[BlockId]] = {
    val stageBlocks = new HashMap[Int, List[BlockId]]
    for ((blockId, stages) <- blockIdToStages) {
      for (s <- stages) {
        val blocks = stageBlocks.getOrElse(s, List[BlockId]())
        stageBlocks.update(s, blocks :+ blockId)
      }
    }
    stageBlocks
  }
}
