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

class MyMove (val blockId: BlockId, val toLevel: StorageLevel) extends Move {

  def operateOn(solution: Solution): Unit = {
      solution.asInstanceOf[MySolution].setStorageLevel(blockId, toLevel)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MyMove]

  override def equals(other: Any): Boolean = other match {
    case that: MyMove =>
      (that canEqual this) &&
        blockId == that.blockId &&
        toLevel == that.toLevel
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(blockId, toLevel)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString(): String = {
    val level = toLevel match {
      case StorageLevel.MEMORY_ONLY => "MEMORY_ONLY"
      case StorageLevel.MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
      case StorageLevel.MEMORY_AND_DISK => "MEMORY_AND_DISK"
      case StorageLevel.DISK_ONLY => "DISK_ONLY"
      case StorageLevel.MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
      case StorageLevel.NONE => "NONE"
    }
    level
  }

}