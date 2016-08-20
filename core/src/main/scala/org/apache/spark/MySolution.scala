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

import scala.collection.mutable.HashMap

class MySolution(val candidateRDDs: List[BlockId],
                 val executorMemory: Long) extends SolutionAdapter {
  val storageLevels: HashMap[BlockId, StorageLevel] =
    HashMap(candidateRDDs.map(r => (r, StorageLevel.MEMORY_ONLY)): _*)
  var executorMemoryVar: Long = 0

//  override def clone: MySolution = {
//    val copy = super.clone().asInstanceOf[MySolution]
//    copy.storageLevels = this.storageLevels.clone()
//    copy.isMem = this.isMem.clone()
//    copy.isDisk = this.isDisk.clone()
//    return copy;
//  }

  def setStorageLevel(blockId: BlockId, toLevel: StorageLevel): Unit = {
    val fromLevel = storageLevels(blockId)
    MyLog.info("Block: " + blockId + " from " + fromLevel + " to " + toLevel)
    storageLevels.update(blockId, toLevel)
  }
}