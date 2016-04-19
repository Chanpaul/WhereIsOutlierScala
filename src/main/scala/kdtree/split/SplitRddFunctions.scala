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

package kdtree.split

 import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * See also Erik Erlandson's DropRDDFunctions class:
 *
 * https://github.com/erikerlandson/spark/blob/469fbf6758959a793e2b21865ff214c9301b02fd/core/src/main/scala/org/apache/spark/rdd/DropRDDFunctions.scala
 *
 * as well as the associated DropRDDFunctionsSuite class:
 *
 * https://github.com/erikerlandson/spark/blob/469fbf6758959a793e2b21865ff214c9301b02fd/core/src/test/scala/org/apache/spark/rdd/DropRDDFunctionsSuite.scala
 *
 * @author Russ Brown
 */
class SplitRddFunctions[T : ClassTag](self: RDD[T]) extends Logging with Serializable {

  /**
   * Split the input RDD at the kth element and return a tuple that represents (1) the kth element,
   * (2) an RDD that comprises the elements to the left of the kth element and (3) an RDD that comprises
   * the elements to the right of the kth element. If splitting is not possible, return (1) None for
   * the kth element, (2) the input RDD and (3) an empty RDD; the positions of the input RDD and the
   * empty RDD in the returned tuple are established by the value of the argument (k).
   * @author Russ Brown
   */
  def splitAt(k: Long): (Option[T], RDD[T], RDD[T]) = {

    // Cannot split because the kth element is to the left of the leftmost (first) element.
    if (k <= 0) {
      return (None, self.sparkContext.emptyRDD[T], self)
    }

    // This case is analogous to DropRDDFunctions.drop(0) and must avoid attempting to locate a partition
    // because rem == 0 and hence the below 'while' loop will not execute and will produce an error.
    if (k == 1) {
      return (Option(takeFirst), self.sparkContext.emptyRDD[T], dropFirst)
    }

    // Locate the partition that includes the element immediately to the left of the kth element.
    val partitionSizes: Array[Int] = getPartitionSizes()
    var rem: Long = k - 1 // Assume that an RDD may comprise greater than 2^31-1 elements.
    var p: Int = 0
    var np: Int = 0
    while (rem > 0  &&  p < partitionSizes.length) {
      np = partitionSizes(p)
      rem -= np
      p += 1
    }

    // Cannot split because the kth element is to the right of the rightmost (last) element.
    if (rem > 0  ||  (rem == 0  &&  p >= self.partitions.length)) {
      return (None, self, self.sparkContext.emptyRDD[T])
    }

    // Create the RDD (rddLeft) that includes all elements of the parent RDD
    // that are to the left of the kth element (if we get here, rem <= 0).
    val pLeft: Int = p - 1
    val pTake: Int = np + rem.toInt // It is assumed that at this point rem < 2^32-1.
    val rddLeft = new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index < pLeft) return firstParent[T].iterator(split, context)
        if (split.index == pLeft) return firstParent[T].iterator(split, context).take(pTake)
        Iterator.empty
      }
    }

    // The kth element is one element to the right of the elements that form rddLeft. If rem < 0,
    // take the kth element from the partition pLeft; otherwise, the kth element is the leftmost
    // (first) element of the partition immediately to the right of the partition pLeft, ignoring
    // any empty partitions.
    val kthElement = if (rem < 0) {
      collectElementsFromOnePartition(pLeft)(pTake)
    } else {
      // Ignore any empty partition.
      while (partitionSizes(p) <= 0 && p < self.partitions.length) {
        p += 1
      }
      // Check whether a kth element exists.
      if (p == self.partitions.length) {
        return (None, rddLeft, self.sparkContext.emptyRDD[T])
      }
      collectElementsFromOnePartition(p)(0)
    }

    // Create the RDD (rddRight) that includes all elements of the parent RDD
    // that are to the right of the kth element; if rem < 0, rddRight begins in
    // the partition pLeft; otherwise, it begins with the second element of the
    // partition immediately to the right of the partition pLeft.
    var pRight: Int = p
    var pDrop: Int = 1
    if (rem < 0) {
      pRight = pLeft
      pDrop = pTake + 1
    }
    val rddRight = new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index > pRight) return firstParent[T].iterator(split, context)
        if (split.index == pRight) return firstParent[T].iterator(split, context).drop(pDrop)
        Iterator.empty
      }
    }

    (Option(kthElement), rddLeft, rddRight)
  }

  /**
   * Get the size of each partition, hopefully in parallel, in contrast to Erik Erlandson's code that gets the partition
   * sizes one at a time. See http://stackoverflow.com/questions/28687149/how-to-get-the-number-of-elements-in-partition
   */
  private def getPartitionSizes(): Array[Int] = self.mapPartitions(iter => Array(iter.size).iterator, true).collect

  /**
   * Return a new RDD formed by dropping the first element of the input RDD.
   */
  private def dropFirst(): RDD[T] = {

    // Search forward to the partition that includes the first element.
    val partitionSizes: Array[Int] = getPartitionSizes()
    var rem: Int = 1 // OK for rem to be Int because looking for 1.
    var p: Int = 0
    var np: Int = 0
    while (rem > 0  &&  p < partitionSizes.length) {
      np = partitionSizes(p)
      rem -= np
      p += 1
    }

    // There is no first element.
    if (rem > 0  ||  (rem == 0  &&  p >= self.partitions.length)) {
      return self.sparkContext.emptyRDD[T]
    }

    // Return an RDD that discounts the first element of the parent RDD
    // (if we get here, rem <= 0).
    val pFirst: Int = p - 1
    val pDrop: Int = np + rem
    new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index > pFirst) return firstParent[T].iterator(split, context)
        if (split.index == pFirst) return firstParent[T].iterator(split, context).drop(pDrop)
        Iterator.empty
      }
    }
  }

  /**
   * Return the first element of the input RDD.
   */
  def takeFirst(): T = {

    // Search forward to the partition that includes the first element, back up one partition and take the first element.
    val partitionSizes: Array[Int] = getPartitionSizes()
    var rem: Int = 1 // OK for rem to be Int because looking for 1.
    var p: Int = 0
    var np: Int = 0
    while (rem > 0  &&  p < partitionSizes.length) {
      np = partitionSizes(p)
      rem -= np
      p += 1
    }
    val firstNonEmptyPartition = collectElementsFromOnePartition(p-1)
    firstNonEmptyPartition(0)
  }

  /**
   * Return the last element of the input RDD.
   */
  def takeLast(): T = {

    // Search backward to the partition that includes the last element, move forward one partition and take the first element.
    val partitionSizes: Array[Int] = getPartitionSizes()
    var rem: Int = 1 // OK for rem to be Int because looking for 1.
    var p: Int = partitionSizes.length-1
    var np: Int = 0
    while (rem > 0  &&  p >= 0) {
      np = partitionSizes(p)
      rem -= np
      p -= 1
    }
    val lastNonEmptyPartition = collectElementsFromOnePartition(p+1)
    lastNonEmptyPartition(lastNonEmptyPartition.length-1)
  }

  /**
   * Return an Array of the elements from one partition.
   */
  private def collectElementsFromOnePartition(p:Int): Array[T] = {
    // Construct a new RDD for which the compute method handles only one partition
    // then collect the elements from that partition.
    new RDD[T](self) {
      override def getPartitions: Array[Partition] = firstParent[T].partitions
      override val partitioner = self.partitioner
      override def compute(split: Partition, context: TaskContext):Iterator[T] = {
        if (split.index == p) return firstParent[T].iterator(split, context)
        Iterator.empty
      }
    }.collect
  }

}