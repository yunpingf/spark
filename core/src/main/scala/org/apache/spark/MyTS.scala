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

import org.apache.spark.storage.{StorageLevel, BlockId}
import org.apache.spark.util.Utils
import org.coinor.opents.{BestEverAspirationCriteria, SimpleTabuList, SingleThreadedTabuSearch}
import tachyon.client.file.TachyonFileSystem
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory

import scala.collection.mutable
import scala.collection.mutable._

object MyTS {
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.app.name", "org.apache.spark.examples.MovieLensALS")
    // HashMap[String, LinkedHashMap[BlockId, HashSet[Int]]]
    val executorIdToParDep = Utils.readFromTachyonFile(TachyonPath.executorIdToParDep, tfs).
      asInstanceOf[LinkedHashMap[String, LinkedHashMap[BlockId, ArrayBuffer[Int]]]]
    MyLog.info("!!!" + executorIdToParDep.toString())
    // GB to byte
    val executorMemory: Long = (1 * 1024 * 1024 * 1024 * 0.75 * 0.5).toLong;
    for ((executorId, blockIdToStages) <- executorIdToParDep) {
      MyLog.info("ExecutorId in Tabu Search: " + executorId)
      // Candidate blocks on this executor
      val candidateBlocks = executorIdToParDep(executorId).keySet.toList

      val objFunc = new MyObjectiveFunction(blockIdToStages)
      val initialSolution = new MySolution(candidateBlocks, executorMemory)
      val moveManager = new MyMoveManager()
      val tabuList = new SimpleTabuList(7) // In OpenTS package

      // Create Tabu Search object
      val tabuSearch = new SingleThreadedTabuSearch(
        initialSolution,
        moveManager,
        objFunc,
        tabuList,
        new BestEverAspirationCriteria(), // In OpenTS package
        false) // maximizing = yes/no; false means minimizing

      // Start solving
      tabuSearch.setIterationsToGo(10)
      tabuSearch.startSolving()

      // Show solution
      val best = tabuSearch.getBestSolution().asInstanceOf[MySolution]
      MyLog.info("Best Solution:\n" + best)
      MyLog.info("The storage level is: " + best.storageLevels)
      var rddResult = Utils.readFromTachyonFile(TachyonPath.rddResult, tfs)
      if (rddResult == null) {
        rddResult = new HashMap[BlockId, StorageLevel]()
      }
      for (elem <- best.storageLevels) {
        rddResult.asInstanceOf[HashMap[BlockId, StorageLevel]].put(elem._1, elem._2)
      }
      Utils.writeToTachyonFile(TachyonPath.rddResult, rddResult, tfs, true)
    }

  }
}
