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
import org.coinor.opents._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MyMoveManager extends MoveManager {
  def getAllMoves(solution: Solution): Array[Move] = {
    val storageLevels: mutable.HashMap[BlockId, StorageLevel] =
      solution.asInstanceOf[MySolution].storageLevels
    val memoryLeft = solution.asInstanceOf[MySolution].memoryLeft

    val size = storageLevels.size
    val moves = new ArrayBuffer[Move]()
    val levels = Array[StorageLevel](StorageLevel.NONE,
      StorageLevel.DISK_ONLY,
      StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY_SER,
      StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK_SER)
    for ((blockId, level) <- storageLevels) {
      for (lev <- levels) {
        val ifContinue = (level.equals(StorageLevel.MEMORY_ONLY) && memoryLeft > 0
          && lev.equals(StorageLevel.MEMORY_AND_DISK)) ||
          (level.equals(StorageLevel.MEMORY_ONLY_SER) && memoryLeft > 0
          && lev.equals(StorageLevel.MEMORY_AND_DISK_SER))
        if (!ifContinue) {
          moves.append(new MyMove(blockId, lev))
        }
      }
    }
    return moves.toArray
  }
}