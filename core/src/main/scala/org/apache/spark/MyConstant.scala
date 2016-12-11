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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.storage.BlockId
import scala.collection.mutable.HashMap

object MyConstant {
  val recordsCount = new HashMap[String, HashMap[BlockId, Int]]
  def put(blockId: BlockId, uuid: String): Unit = {
    val map = recordsCount.getOrElseUpdate(uuid, new HashMap[BlockId, Int])
    val i = map.getOrElseUpdate(blockId, 0)
    map.put(blockId, i + 1)
  }
}
