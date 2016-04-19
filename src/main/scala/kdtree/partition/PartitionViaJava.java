/**
 * Copyright (c) 2015, 2016 Russell A. Brown
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

package kdtree.partition;

import java.util.List;

import scala.Option;

import kdtree.box.BoundingBox;
import kdtree.kdtree.KdNode;

/**
 * Build a 2-d tree via Java wherein arrays are partitioned by xMin and yMin.
 */
public class PartitionViaJava {

    // The following method builds one node of a k-d tree by partitioning arrays by xMin and returns
    // a BoundingBox that describes the node id and the bounding region that surrounds the node.
    // As a side effect, a BoundingBox that describes the node id and its bounding box, plus two
    // additional BoundingBoxes that describe the child nodes and the bounding regions that surround
    // them, are added to a List<KdNode>. This method is re-entrant, so it may be executed
    // simultaneously via multiple threads without creating a race condition.
    //
    // Note: An array is indexed from startIndex to endIndex inclusive.
    public static BoundingBox partitionByXminViaJava(BoundingBox[] xMinCollected,
                                                     BoundingBox[] yMinCollected,
                                                     BoundingBox[] temporary,
                                                     int startIndex,
                                                     int endIndex,
                                                     List<KdNode> kdNodes) {

        if (startIndex == endIndex) {
            // Add the terminal node with no children to the List<KdNode> and return the node whose
            // bounding box is by definition its bounding region, so no need to create a bounding region.
            BoundingBox node = xMinCollected[startIndex];
            Option<BoundingBox> none = Option.apply(null);
            kdNodes.add(new KdNode(node, none, none));
            return node;
        } else if (endIndex > startIndex) {
            // Partition the xMinCollected Array into low and high halves. The median will provide the id for the KdNode.
            int medianIndex = startIndex + ( (endIndex - startIndex) >> 1 );
            BoundingBox node = xMinCollected[medianIndex];

            // Partition the yMinCollected Array into low and high halves for the two recursive calls to the
            // partitionArraysByYmin method by copying into the temporary array.
            int loIndex = startIndex - 1;
            int hiIndex = medianIndex;
            for (int i = startIndex; i <= endIndex; ++i) {
                if (yMinCollected[i].xMinLessThan(node)) {
                    temporary[++loIndex] = yMinCollected[i];
                } else if (yMinCollected[i].xMinMoreThan(node)) {
                    temporary[++hiIndex] = yMinCollected[i];
                }
            }

            // Initialize the bounding region from the node's bounding box then refine it from the node's children.
            long xMin = node.getXmin();
            long yMin = node.getYmin();
            long xMax = node.getXmax();
            long yMax = node.getYmax();

            // Create the low child that comprises an id and a bounding region via partitioning by yMin.
            // The yMincollected and temporary arrays are swapped in the partitionByYmin() call.
            BoundingBox loChild = null;
            if (loIndex >= startIndex) {
                BoundingBox child = partitionByYminViaJava(xMinCollected, temporary, yMinCollected, startIndex, loIndex, kdNodes);
                loChild = child;
                xMin = Math.min(xMin, child.getXmin());
                yMin = Math.min(yMin, child.getYmin());
                xMax = Math.max(xMax, child.getXmax());
                yMax = Math.max(yMax, child.getYmax());
            }

            // Create the high child that comprises an id and a bounding region via partitioning by yMin.
            // The yMincollected and temporary arrays are swapped in the partitionByYmin() call.
            BoundingBox hiChild = null;
            if (hiIndex > medianIndex) {
                BoundingBox child = partitionByYminViaJava(xMinCollected, temporary, yMinCollected, medianIndex + 1, hiIndex, kdNodes);
                hiChild = child;
                xMin = Math.min(xMin, child.getXmin());
                yMin = Math.min(yMin, child.getYmin());
                xMax = Math.max(xMax, child.getXmax());
                yMax = Math.max(yMax, child.getYmax());
            }

            // Add the node with any children to the List<KdNode>.
            Option<BoundingBox> optionLoChild = Option.apply(loChild);
            Option<BoundingBox> optionHiChild = Option.apply(hiChild);
            kdNodes.add(new KdNode(node, optionLoChild, optionHiChild));

            // Return the bounding region for this node.
            return new BoundingBox(node.getId(), xMin, yMin, xMax, yMax);
        } else {
            // endIndex < startIndex, which is impossible in view of the above check of loIndex and hiIndex, so check for this condition last.
            throw new RuntimeException("indices out of order: startIndex = $startIndex  endIndex = $endIndex");
        }
    }

    // The following method builds one node of a k-d tree by partitioning arrays by yMin and returns
    // a BoundingBox that describes the node id and the bounding region that surrounds the node.
    // As a side effect, a BoundingBox that describes the node id and its bounding box, plus two
    // additional BoundingBoxes that describe the child nodes and the bounding regions that surround
    // them, are added to a List<KdNode>. This method is re-entrant, so it may be executed
    // simultaneously via multiple threads without creating a race condition.
    //
    // Note: An array is indexed from startIndex to endIndex inclusive.
    public static BoundingBox partitionByYminViaJava(BoundingBox[] xMinCollected,
                                                     BoundingBox[] yMinCollected,
                                                     BoundingBox[] temporary,
                                                     int startIndex,
                                                     int endIndex,
                                                     List<KdNode> kdNodes) {

        if (startIndex == endIndex) {
            // Add the terminal node with no children to the List<KdNode> and return the node whose
            // bounding box is by definition its bounding region, so no need to create a bounding region.
            BoundingBox node = yMinCollected[startIndex];
            Option<BoundingBox> none = Option.apply(null);
            kdNodes.add(new KdNode(node, none, none));
            return node;
        } else if (endIndex > startIndex) {
            // Partition the yMinCollected Array into low and high halves. The median will provide the id for the KdNode.
            int medianIndex = startIndex + ( (endIndex - startIndex) >> 1 );
            BoundingBox node = yMinCollected[medianIndex];

            // Partition the xMinCollected Array into low and high halves for the two recursive calls to the
            // partitionArraysByXmin method by copying into the temporary array.
            int loIndex = startIndex - 1;
            int hiIndex = medianIndex;
            for (int i = startIndex; i <= endIndex; ++i) {
                if (xMinCollected[i].yMinLessThan(node)) {
                    temporary[++loIndex] = xMinCollected[i];
                } else if (xMinCollected[i].yMinMoreThan(node)) {
                    temporary[++hiIndex] = xMinCollected[i];
                }
            }

            // Initialize the bounding region from the node's bounding box then refine it from the node's children.
            long xMin = node.getXmin();
            long yMin = node.getYmin();
            long xMax = node.getXmax();
            long yMax = node.getYmax();

            // Create the low child that comprises an id and a bounding region via partitioning by xMin.
            // The xMincollected and temporary arrays are swapped in the partitionByXmin() call.
            BoundingBox loChild = null;
            if (loIndex >= startIndex) {
                BoundingBox child = partitionByXminViaJava(temporary, yMinCollected, xMinCollected, startIndex, loIndex, kdNodes);
                loChild = child;
                xMin = Math.min(xMin, child.getXmin());
                yMin = Math.min(yMin, child.getYmin());
                xMax = Math.max(xMax, child.getXmax());
                yMax = Math.max(yMax, child.getYmax());
            }

            // Create the high child that comprises an id and a bounding region via partitioning by xMin.
            // The xMincollected and temporary arrays are swapped in the partitionByXmin() call.
            BoundingBox hiChild = null;
            if (hiIndex > medianIndex) {
                BoundingBox child = partitionByXminViaJava(temporary, yMinCollected, xMinCollected, medianIndex + 1, hiIndex, kdNodes);
                hiChild = child;
                xMin = Math.min(xMin, child.getXmin());
                yMin = Math.min(yMin, child.getYmin());
                xMax = Math.max(xMax, child.getXmax());
                yMax = Math.max(yMax, child.getYmax());
            }

            // Add the node with any children to the List<KdNode>.
            Option<BoundingBox> optionLoChild = Option.apply(loChild);
            Option<BoundingBox> optionHiChild = Option.apply(hiChild);
            kdNodes.add(new KdNode(node, optionLoChild, optionHiChild));

            // Return the bounding region for this node.
            return new BoundingBox(node.getId(), xMin, yMin, xMax, yMax);
        } else {
            // endIndex < startIndex, which is impossible in view of the above check of loIndex and hiIndex, so check for this condition last.
            throw new RuntimeException("indices out of order: startIndex = $startIndex  endIndex = $endIndex");
        }
    }

}
