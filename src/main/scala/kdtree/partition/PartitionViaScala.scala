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

package kdtree.partition

import kdtree.box.BoundingBox
import kdtree.kdtree.KdNode

/**
 * Build a 2-d tree wherein arrays are partitioned by xMin and yMin.
 */
object PartitionViaScala {

  // The following method builds one node of a k-d tree by partitioning arrays by xMin and returns
  // a BoundingBox that describes the node id and the bounding region that surrounds the node.
  // As a side effect, a BoundingBox that describes the node id and its bounding box, plus two
  // additional BoundingBoxes that describe the child nodes and the bounding regions that surround
  // them, are prepended to a List[KdNode]. This method is re-entrant, so it may be executed
  // simultaneously via multiple threads without creating a race condition.
  //
  // Note: An array is indexed from startIndex to endIndex inclusive.
  def partitionByXminViaScala(xMinCollected: Array[BoundingBox],
                              yMinCollected: Array[BoundingBox],
                              temporary: Array[BoundingBox],
                              startIndex: Int,
                              endIndex: Int,
                              listPrepender: (KdNode) => Boolean): BoundingBox = {

    if (startIndex == endIndex) {
      // Prepend the terminal node with no children to the List[KdNode] and return the node whose
      // bounding box is by definition its bounding region, so no need to create a bounding region.
      val node = xMinCollected(startIndex)
      listPrepender(new KdNode(node, None, None))
      node
    } else if (endIndex > startIndex) {
      // Partition the xMinCollected Array into low and high halves. The median will provide the id for the KdNode.
      val medianIndex = startIndex + ( (endIndex - startIndex) >> 1 )
      val node: BoundingBox = xMinCollected(medianIndex)

      // Partition the yMinCollected Array into low and high halves for the two recursive calls to the
      // partitionArraysByYminViaScala method by copying into the temporary array.
      var loIndex = startIndex - 1
      var hiIndex = medianIndex
      for (i <- startIndex to endIndex) {
        if (yMinCollected(i).xMinLessThan(node)) {
          loIndex += 1
          temporary(loIndex) = yMinCollected(i)
        } else if (yMinCollected(i).xMinMoreThan(node)) {
          hiIndex += 1
          temporary(hiIndex) = yMinCollected(i)
        }
      }

      // Initialize the bounding region from the node's bounding box then refine it from the node's children.
      var xMin = node.xMin
      var yMin = node.yMin
      var xMax = node.xMax
      var yMax = node.yMax

      // Create the low child that comprises an id and a bounding region via partitioning by yMin.
      // The yMincollected and temporary arrays are swapped in the partitionByYminViaScala() call.
      val loChild: Option[BoundingBox] = if (loIndex >= startIndex) {
        val child = partitionByYminViaScala(xMinCollected, temporary, yMinCollected, startIndex, loIndex, listPrepender)
        xMin = Math.min(xMin, child.xMin)
        yMin = Math.min(yMin, child.yMin)
        xMax = Math.max(xMax, child.xMax)
        yMax = Math.max(yMax, child.yMax)
        Option(child)
      } else {
        None
      }

      // Create the high child that comprises an id and a bounding region via partitioning by yMin.
      // The yMincollected and temporary arrays are swapped in the partitionByYmin() call.
      val hiChild = if (hiIndex > medianIndex) {
        val child = partitionByYminViaScala(xMinCollected, temporary, yMinCollected, medianIndex + 1, hiIndex, listPrepender)
        xMin = Math.min(xMin, child.xMin)
        yMin = Math.min(yMin, child.yMin)
        xMax = Math.max(xMax, child.xMax)
        yMax = Math.max(yMax, child.yMax)
        Option(child)
      } else {
        None
      }

      // Prepend the node with any children to the List[KdNode].
      listPrepender(new KdNode(node, loChild, hiChild))

      // Return the bounding region for this node.
      new BoundingBox(node.id, xMin, yMin, xMax, yMax)
    } else {
      // endIndex < startIndex, which is impossible in view of the above check of loIndex and hiIndex, so check for this condition last.
      throw new RuntimeException(s"indices out of order: startIndex = $startIndex  endIndex = $endIndex")
    }
  }

  // The following method builds one node of a k-d tree by partitioning arrays by yMin and returns
  // a BoundingBox that describes the node id and the bounding region that surrounds the node.
  // As a side effect, a BoundingBox that describes the node id and its bounding box, plus two
  // additional BoundingBoxes that describe the child nodes and the bounding regions that surround
  // them, are prepended to a List[KdNode]. This method is re-entrant, so it may be executed
  // simultaneously via multiple threads without creating a race condition.
  //
  // Note: An array is indexed from startIndex to endIndex inclusive.
  def partitionByYminViaScala(xMinCollected: Array[BoundingBox],
                              yMinCollected: Array[BoundingBox],
                              temporary: Array[BoundingBox],
                              startIndex: Int,
                              endIndex: Int,
                              listPrepender: (KdNode) => Boolean): BoundingBox = {

    if (startIndex == endIndex) {
      // Prepend the terminal node with no children to the List[KdNode] and return the node whose
      // bounding box is by definition its bounding region, so no need to create a bounding region.
      // Split the xMinCollected Array into low and high halves. The median will provide the id for the KdNode.
      val node = yMinCollected(startIndex)
      listPrepender(new KdNode(node, None, None))
      node
    } else if (endIndex > startIndex) {
      // Partition the yMinCollected Array into low and high halves. The median will provide the id for the KdNode.
      val medianIndex = startIndex + ( (endIndex - startIndex) >> 1 )
      val node: BoundingBox = yMinCollected(medianIndex)

      // Partition the xMinCollected Array into low and high halves for the two recursive calls to the
      // partitionArraysByXminViaScala method by copying into the temporary array.
      var loIndex = startIndex - 1
      var hiIndex = medianIndex
      for (i <- startIndex to endIndex) {
        if (xMinCollected(i).yMinLessThan(node)) {
          loIndex += 1
          temporary(loIndex) = xMinCollected(i)
        } else if (xMinCollected(i).yMinMoreThan(node)) {
          hiIndex += 1
          temporary(hiIndex) = xMinCollected(i)
        }
      }

      // Initialize the bounding region from the node's bounding box then refine it from the node's children.
      var xMin = node.xMin
      var yMin = node.yMin
      var xMax = node.xMax
      var yMax = node.yMax

      // Create the low child that comprises an id and a bounding region via partitioning by xMin.
      // The xMincollected and temporary arrays are swapped in the partitionByXminViaScala() call.
      val loChild: Option[BoundingBox] = if (loIndex >= startIndex) {
        val child = partitionByXminViaScala(temporary, yMinCollected, xMinCollected, startIndex, loIndex, listPrepender)
        xMin = Math.min(xMin, child.xMin)
        yMin = Math.min(yMin, child.yMin)
        xMax = Math.max(xMax, child.xMax)
        yMax = Math.max(yMax, child.yMax)
        Option(child)
      } else {
        None
      }

      // Create the high child that comprises an id and a bounding region via partitioning by xMin.
      // The xMincollected and temporary arrays are swapped in the partitionByXmin() call.
      val hiChild = if (hiIndex > medianIndex) {
        val child = partitionByXminViaScala(temporary, yMinCollected, xMinCollected, medianIndex + 1, hiIndex, listPrepender)
        xMin = Math.min(xMin, child.xMin)
        yMin = Math.min(yMin, child.yMin)
        xMax = Math.max(xMax, child.xMax)
        yMax = Math.max(yMax, child.yMax)
        Option(child)
      } else {
        None
      }

      // Prepend the node with any children to the List[KdNode].
      listPrepender(new KdNode(node, loChild, hiChild))

      // Return the bounding region for this node.
      new BoundingBox(node.id, xMin, yMin, xMax, yMax)
    } else {
      // endIndex < startIndex, which is impossible in view of the above check of loIndex and hiIndex, so check for this condition last.
      throw new RuntimeException(s"indices out of order: startIndex = $startIndex  endIndex = $endIndex")
    }
  }

}
