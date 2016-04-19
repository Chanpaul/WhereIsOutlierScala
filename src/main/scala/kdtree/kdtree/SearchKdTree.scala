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

package kdtree.kdtree

import kdtree.box.BoundingBox
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Search a k-d tree.
 */
object SearchKdTree extends Serializable {

  // The following method searches a k-d tree.
  def searchKdTree(root: BoundingBox,
                   kdTree: RDD[KdNode],
                   query: RDD[BoundingBox],
                   numcores: Int,
                   persistenceLevel: StorageLevel)(implicit sc: SparkContext): RDD[(Long, List[Long])] = {

    // Accumulate the results of the search into this RDD.
    var result = sc.parallelize(Array[(Long, Long)]())

    // Direct the first iteration of the search to the root of the k-d tree.
    var search: RDD[(Long, BoundingBox)] = query.map(q => (root.id, q)).persist(persistenceLevel)

    // Represent the k-d tree RDD as a pair RDD using the node id as the key.  Because the k-d tree has a number
    // of partitions equal to the number of cores (see the appendToList() and flushList() methods above), it will
    // not be shuffled by the join() method below.  However, this approach may require that one compute node process
    // all of the results of the join() method for the first iteration of the 'while' loop below, that two compute
    // nodes process all of the results of the join() method for the second iteration of the 'while' loop, etc.
    // For this reason, ensure that the join() method creates a number of partitions equal to the number of cores.
    val tree: RDD[(Long, KdNode)] = kdTree.map(n => (n.node.id, n)).persist(persistenceLevel)

    // Search iteratively until the search RDD is empty.
    while (search.count > 0) {

      // Visit the KdNodes of the k-d tree that are specified by the search RDD and partition the result to all cores.
      val visit: RDD[(Long, (KdNode, BoundingBox))] = tree.join(search, numcores).persist(persistenceLevel)

      // Accumulate the intersection result for this iteration. Use flatMapValues() to avoid reshuffling, then
      // use map() to keep only the value, then use coalesce() to avoid increasing the number of partitions.
      result = result.union(
        visit.flatMapValues(v => {
          val bb = v._2
          val node = v._1
          if (bb.intersectsBox(node.node)) {
            List((bb.id, node.node.id))
          } else {
            List()
          }
        }).map(_._2) // Keep only the value, which hopefully doesn't require reshuffling.
      ).coalesce(numcores).persist(persistenceLevel)

      // Determine which children (if any) to search during the next iteration. Use flatMapValues() to avoid reshuffling.
      search = visit.flatMapValues(v => {
        val bb = v._2
        val node = v._1
        var children = List[(Long, BoundingBox)]()
        if (node.loChild != None) {
          val child = node.loChild.getOrElse(throw new RuntimeException(s"low child != None but cannot get child"))
          if (bb.intersectsRegion(child)) {
            children ::= (child.id, bb)
          }
        }
        if (node.hiChild != None) {
          val child = node.hiChild.getOrElse(throw new RuntimeException(s"high child != None but cannot get child"))
          if (bb.intersectsRegion(child)) {
            children ::= (child.id, bb)
          }
        }
        children
      }).map(_._2).persist(persistenceLevel) // Keep only the value, which hopefully doesn't require reshuffling.
    }

    // Group the results by search id and convert to a List.
    result.groupByKey.map(r => (r._1, r._2.toList))
  }
  
}
