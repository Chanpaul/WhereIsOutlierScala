/**
 * Copyright (c) 2015, 2016, Russell A. Brown
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSEARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package kdtree.build

import kdtree.box.BoundingBox
import kdtree.kdtree.{CreateKdTree, SearchKdTree}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import kdtree.util.ParseArgs
import kdtree.util.ParseArgs._

/**
 * Build and search a two-dimensional k-d tree.
 */
object BuildAndSearchKdTree extends Serializable {

  // Persist RDDs at the MEMORY_AND_DISK_SER level and to minimize garbage collection; may require Kryo.
  final val PERSISTENCE_LEVEL = StorageLevel.MEMORY_AND_DISK_SER

  /**
   * It does not appear necessary to provide any information to the main() function other than 'args'
   * and the implicit SparkContext parameter that is provided by ScalaTest.  However, this implicit
   * parameter is incompatible with execution via the spark-submit script but is OK for a ScalaTest.
   */
  def main(args: Array[String])(implicit sc: SparkContext): RDD[(Long, List[Long])] = {

    val arguments: OptionMap = ParseArgs.parse(args)
    val input: String = arguments.get('input).getOrElse{throw new RuntimeException(s"no --input specified")}.toString
    val numCores: Int = arguments.get('partitions).getOrElse{throw new RuntimeException(s"no --partiitons specified")}.toString.toInt
    val cutoff: Int = arguments.get('cutoff).getOrElse(throw new RuntimeException(s"no --cutoff specified")).toString.toInt
    val partitionViaJava: Boolean = arguments.get('java).getOrElse(s"true").toString.toBoolean

    // Parse the input file to create an RDD[BoundingBox] and persist that RDD.
    val boundingBoxes = sc.textFile(input, numCores)
      .map(_.split('\t'))
      .map(bs => bs.map(be => be.toLong))
      .zipWithIndex
      .map(bz => (bz._2.toLong, bz._1)) // Ensure that the index is Long and swap its position with the bounding box.
      .map(bz => BoundingBox.createBoundingBox(bz._1, bz._2(0), bz._2(1), bz._2(2), bz._2(3)))
      .persist(PERSISTENCE_LEVEL)

    // Build then search the k-d tree.
    val (root, kdNodes, numKdNodes, sortTime, upperTreeTime, lowerTreeTime, listTime) =
      CreateKdTree.createKdTree(boundingBoxes, numCores, cutoff, PERSISTENCE_LEVEL, false, partitionViaJava)
    val searchResult = SearchKdTree.searchKdTree(root, kdNodes, boundingBoxes, numCores, PERSISTENCE_LEVEL)

   // Return the search result.
    searchResult
  }
}

