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

package kdtree.kdtree

import java.util.ArrayList

import kdtree.box.BoundingBox
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import kdtree.partition.{PartitionViaScala, PartitionViaJava}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import kdtree.split.RddToSplitRddFunctions

/**
 * This class contains methods that are not re-entrant because of the List[KdNode],
 * so a unique instance of this class must be created for each thread.
 *
 * Note: Scala threads are provided by the
 *
 * import scala.concurrent.ExecutionContext.Implicits.global
 *
 * An alternative would be to create a scala.concurrent.ExecutionContextExecutorService from a
 * java.util.concurrent.Executors.newFixedThreadPool or .newCachedThreadPool; however, the
 * scala.concurrent.ExecutionContext.Implicits.global automatically provides a number of threads
 * equal to the number of CPU cores (including a factor of 2x for Intel i7 hyperthreads).
 */
 class CreateKdTree{

  def createFutureXminViaScala(xMinCollected: Array[BoundingBox],
                               yMinCollected: Array[BoundingBox],
                               temporary: Array[BoundingBox],
                               startIndex: Int,
                               endIndex: Int): Future[List[KdNode]] = Future {

    var kdNodes = List[KdNode]()

    def prependToList(kdNode: KdNode): Boolean = {
      kdNodes = kdNode :: kdNodes
      true
    }

    PartitionViaScala.partitionByXminViaScala(xMinCollected, yMinCollected, temporary, startIndex, endIndex, prependToList)
    kdNodes
  }

  def createFutureYminViaScala(xMinCollected: Array[BoundingBox],
                               yMinCollected: Array[BoundingBox],
                               temporary: Array[BoundingBox],
                               startIndex: Int,
                               endIndex: Int): Future[List[KdNode]] = Future {

    var kdNodes = List[KdNode]()

    def prependToList(kdNode: KdNode): Boolean = {
      kdNodes = kdNode :: kdNodes
      true
    }

    PartitionViaScala.partitionByYminViaScala(xMinCollected, yMinCollected, temporary, startIndex, endIndex, prependToList)
    kdNodes
  }

  def createFutureXminViaJava(xMinCollected: Array[BoundingBox],
                              yMinCollected: Array[BoundingBox],
                              temporary: Array[BoundingBox],
                              startIndex: Int,
                              endIndex: Int): Future[List[KdNode]] = Future {

    val kdNodes = new ArrayList[KdNode](endIndex - startIndex + 1) // Allocate for all of the KdNodes.

    PartitionViaJava.partitionByXminViaJava(xMinCollected, yMinCollected, temporary, startIndex, endIndex, kdNodes)
    kdNodes.toList
  }

  def createFutureYminViaJava(xMinCollected: Array[BoundingBox],
                              yMinCollected: Array[BoundingBox],
                              temporary: Array[BoundingBox],
                              startIndex: Int,
                              endIndex: Int): Future[List[KdNode]] = Future {

    val kdNodes = new ArrayList[KdNode](endIndex - startIndex + 1) // Allocate for all of the KdNodes.

    PartitionViaJava.partitionByYminViaJava(xMinCollected, yMinCollected, temporary, startIndex, endIndex, kdNodes)
    kdNodes.toList
  }

}

/**
 * Build a 4-d tree wherein RDDs are partitioned by xMin, yMin, xMax and yMax
 * that comprises 2-d subtrees wherein arrays are partitioned on xMin and yMin.
 */
object CreateKdTree extends Serializable with RddToSplitRddFunctions {

  // The following k-d tree-building method partitions by xMin, yMin, xMax and yMax cyclically.
  def createKdTree(boundingBoxes: RDD[BoundingBox],
                   partitions: Int,
                   arrayCutoff: Int,
                   persistenceLevel: StorageLevel,
                   blockUntilUnpersisted: Boolean = true,
                   partitionViaJava: Boolean = false)(implicit sc: SparkContext): (BoundingBox, RDD[KdNode], Long, Long, Long, Long, Long) = {

    // Specify the cyclic permutation of the BoundingBox limits via an enum.
    object Permutation extends Enumeration {
      type Permutation = Value
      val XMIN, YMIN, XMAX, YMAX = Value
    }

    import Permutation._

    // Count the number of bounding boxes but don't use count that causes a StackOverflowError for even a moderately-sized RDD.
    val numberOfBoxes: Long = boundingBoxes.mapPartitions(iter => Array(iter.size).iterator, true).collect.sum

    // Aggregate the KdNodes into this RDD[KdNode] that is empty initially.
    var kdNodeRdd = sc.parallelize(Array[KdNode]())
    var kdNodeRddIsEmpty = true

    // Prepend the KdNodes to this List[KdNode].
    var kdNodeList = List[KdNode]()

    // Prepend the Future[List[KdNode]] to this List.
    val futures = new ListBuffer[Future[List[KdNode]]]

    // Prepend a KdNode to the List[KdNode] and if the List length equals or exceeds
    // the greater of partitions and arrayCutoff, use the zipPartitions() method to
    // merge the List into the RDD[KdNode] such that the number of partitions is
    // preserved and all partitions have an equal number of elements (to within one
    // element) in order to balance the load during k-d tree search.
    def prependToList(node: KdNode) = {
      kdNodeList = node :: kdNodeList
      if (kdNodeList.length >= Math.max(arrayCutoff, partitions)) {
        kdNodeRdd = if (kdNodeRddIsEmpty) {
          kdNodeRddIsEmpty = false
          sc.parallelize(kdNodeList, partitions)
        } else {
          kdNodeRdd.zipPartitions(sc.parallelize(kdNodeList, partitions), preservesPartitioning=true)((iter, iter2) => iter++iter2)
        }.persist(persistenceLevel)
        kdNodeList = List[KdNode]()
      }
    }

    // If KdNodes remain in the List[KdNode], use the zipPartitions() method to merge the
    // List into the RDD[KdNode] such that the number of partitions is preserved and all
    // partitions have an equal number of elements (to within one element) in order to
    // balance the load during k-d tree search.
    def flushList() = {
      if (kdNodeList.nonEmpty) {
        kdNodeRdd = if (kdNodeRddIsEmpty) {
          kdNodeRddIsEmpty = false
          sc.parallelize(kdNodeList, partitions)
        } else {
          kdNodeRdd.zipPartitions(sc.parallelize(kdNodeList, partitions), preservesPartitioning=true)((iter, iter2) => iter++iter2)
        }.persist(persistenceLevel)
        kdNodeList = List[KdNode]()
      }
    }

    // The following method builds one node of a k-d tree.  As a side effect, a BoundingBox that describes
    // the node id and its bounding box, plus two additional BoundingBoxes that describe the child nodes
    // and the bounding regions that surround the child nodes, are prepended to a List[KdNode]. As
    // another side effect, the q, r and s RDD[BoundingBox] arguments are unpersisted after subdivision
    // so that persistence of only twice the size of the original RDDs is effected at any level of recursion.
    //
    // Note: An RDD is indexed from 1 to numBoxes inclusive.
    def buildKdNode(node: BoundingBox,
                    pLo: RDD[BoundingBox],
                    pHi: RDD[BoundingBox],
                    medianIndex: Long,
                    q: RDD[BoundingBox],
                    r: RDD[BoundingBox],
                    s: RDD[BoundingBox],
                    isLess: (BoundingBox, BoundingBox) => Boolean,
                    isMore: (BoundingBox, BoundingBox) => Boolean,
                    numBoxes: Long,
                    nextPermutation: Permutation) = {

      if (numBoxes > 1) {

        // Subdivide the q, r and s RDDs into low and high halves and persist their halves for the two recursive calls
        // to the buildKdTree method. Unpersist the q, r and s RDDs after their low and high halves have been created
        // and block on unpersistence if requested.
        val qLo = q.filter(bb => isLess(bb, node)).persist(persistenceLevel)
        val rLo = r.filter(bb => isLess(bb, node)).persist(persistenceLevel)
        val sLo = s.filter(bb => isLess(bb, node)).persist(persistenceLevel)
        val qHi = q.filter(bb => isMore(bb, node)).persist(persistenceLevel)
        val rHi = r.filter(bb => isMore(bb, node)).persist(persistenceLevel)
        val sHi = s.filter(bb => isMore(bb, node)).persist(persistenceLevel)
        q.unpersist(blockUntilUnpersisted)
        r.unpersist(blockUntilUnpersisted)
        s.unpersist(blockUntilUnpersisted)

        // Create the low child that comprises an id and a bounding region. Permute the p, q, r and s RDDs
        // cyclically as q->p, r->q, s->r and p->s in the arguments to the recursive call to buildKdTree().
        val loBoxes = medianIndex - 1
        val loChild: Option[BoundingBox] = if (loBoxes > 0) {
          val loNode = buildKdTree(qLo, rLo, sLo, pLo, loBoxes, nextPermutation)
          Option(loNode)
        } else {
          None
        }

        // Create the high child that comprises an id and a bounding region. Permute the p, q, r and s RDDs
        // cyclically as q->p, r->q, s->r and p->s in the arguments to the recursive call to buildKdTree().
        val hiBoxes = numBoxes - medianIndex
        val hiChild = if (hiBoxes > 0) {
          val hiNode = buildKdTree(qHi, rHi, sHi, pHi, hiBoxes, nextPermutation)
          Option(hiNode)
        } else {
          None
        }

        // Aggregate the node with any children to the RDD[KdNode] and return the node id and its bounding region.
        prependToList(new KdNode(node, loChild, hiChild))
      } else {
        throw new RuntimeException(s"number of boxes = $numBoxes")
      }
    }

    // The following method calls one of the four combinations of the buildKdNode() method,
    // specifies cyclically the next permutation, and returns the bounding region for the k-d node.
    // In addition, if the p, q, r and s RDDs contain only one BoundingBox, prepend that BoundingBox
    // to the List[BoundingBox] and unpersist the p, q, r and s RDDs as side effects of this
    // buildKdTree() method.  If the p, q, r and s RDDs contain more than one BoundingBox, unpersist
    // the p RDD after its low and high halves have been created so that persistence of only twice
    // the size of the original RDD is effected at any level of recursion.
    //
    // Note: An RDD is indexed from 1 to numBoxes inclusive.
    def buildKdTree(p: RDD[BoundingBox],
                    q: RDD[BoundingBox],
                    r: RDD[BoundingBox],
                    s: RDD[BoundingBox],
                    numBoxes: Long,
                    permutation: Permutation): BoundingBox = {

      if (numBoxes > arrayCutoff) {
        if (numBoxes > 1) {
          // Subdivide the p RDD into low and high halves and persist its halves for the recursive calls to
          // the buildKdNode method. Unpersist the p RDD as soon as the BoundingBox that defines the bounding
          // region is constructed (the q, r and s RDDs will be unpersisted as soon as they have been subdivided
          // within the buildKdNode() method). Return the bounding region defined by the p, q, r and s RDDs,
          // which are permuted cyclically from the xMin-, yMin-, xMax- and yMax-sorted RDDs.
          val medianIndex = 1 + ((numBoxes - 1) >> 1)
          val (nodeOption: Option[BoundingBox], pLo: RDD[BoundingBox], pHi: RDD[BoundingBox]) = p.splitAt(medianIndex)
          pLo.persist
          pHi.persist
          val node = nodeOption.getOrElse(throw new RuntimeException(s"no bounding box"))
          permutation match {
            case XMIN => {
              val box = new BoundingBox(node.id, p.takeFirst.xMin, q.takeFirst.yMin, r.takeLast.xMax, s.takeLast.yMax)
              p.unpersist(blockUntilUnpersisted)
              buildKdNode(node, pLo, pHi, medianIndex, q, r, s, BoundingBox.xMinIsLess, BoundingBox.xMinIsMore, numBoxes, YMIN)
              box
            }
            case YMIN => {
              val box = new BoundingBox(node.id, s.takeFirst.xMin, p.takeFirst.yMin, q.takeLast.xMax, r.takeLast.yMax)
              p.unpersist(blockUntilUnpersisted)
              buildKdNode(node, pLo, pHi, medianIndex, q, r, s, BoundingBox.yMinIsLess, BoundingBox.yMinIsMore, numBoxes, XMAX)
              box
            }
            case XMAX => {
              val box = new BoundingBox(node.id, r.takeFirst.xMin, s.takeFirst.yMin, p.takeLast.xMax, q.takeLast.yMax)
              p.unpersist(blockUntilUnpersisted)
              buildKdNode(node, pLo, pHi, medianIndex, q, r, s, BoundingBox.xMaxIsLess, BoundingBox.xMaxIsMore, numBoxes, YMAX)
              box
            }
            case YMAX => {
              val box = new BoundingBox(node.id, q.takeFirst.xMin, r.takeFirst.yMin, s.takeLast.xMax, p.takeLast.yMax)
              p.unpersist(blockUntilUnpersisted)
              buildKdNode(node, pLo, pHi, medianIndex, q, r, s, BoundingBox.yMaxIsLess, BoundingBox.yMaxIsMore, numBoxes, XMIN)
              box
            }
            case _ => throw new RuntimeException(s"illegal permutation = $permutation")
          }
        } else if (numBoxes == 1) {
          // Retrieve the first (and only) element of the p RDD, which element is a leaf node of the k-d tree.
          val boundingBox = p.takeFirst
          // Unpersist the p, q, r and s RDDs and block on unpersistence if requested.
          p.unpersist(blockUntilUnpersisted)
          q.unpersist(blockUntilUnpersisted)
          r.unpersist(blockUntilUnpersisted)
          s.unpersist(blockUntilUnpersisted)
          // Prepend the first element of the p RDD to the List[BoundingBox] and return the first element,
          // whose bounding box is identical to the bounding region for this leaf node.
          prependToList(new KdNode(boundingBox, None, None))
          boundingBox
        } else {
          throw new RuntimeException(s"number of boxes = $numBoxes")
        }
      } else {
        // Convert the xMin- and yMin-sorted RDDs to arrays and use an asynchronous thread to subdivide these two
        // arrays, thus transitioning from building a 4-d tree to building a 2-d tree, which may be searched as
        // efficiently as a 4-d tree. The only reason for which this buildKdTree() method constructs a 4-d tree
        // instead of a 2-d tree is that the xMin-, yMin-, xMax- and yMax-sorted RDDs provide the extent of the
        // bounding region for each subtree.  In constrast, the partitionByXmin() and partitionByYmin() methods
        // construct the bounding region upon unwinding the recursion and return that bounding region from each
        // level of recursion; this more efficient approach does not require the xMax- and yMax-sorted RDDs.
        // This buildKdTree() method cannot return a bounding region from each level of recursion because launching
        // the asynchronous thread returns a Future, so this method constructs a bounding region from the RDDs.
        //
        // NOTE: The BoundingBox must be calculated from the median element of the xMin- or yMin-sorted array,
        // where the address of that median element is generated in an identical manner to its generation in the
        // partitionByXmin() and partitionByYmin() methods. Also, for the XMIN and XMAX cases, the median element
        // of the xMin-sorted array is used, whereas for the YMIN and YMAX cases, the median element of the
        // yMin-sorted array is used.
        permutation match {
          case XMIN => {
            val pCollected: Array[BoundingBox] = p.collect
            val qCollected: Array[BoundingBox] = q.collect
            val temporary = new Array[BoundingBox](pCollected.length)
            val node: BoundingBox = pCollected((pCollected.length - 1) >> 1)
            val box = BoundingBox(node.id, p.takeFirst.xMin, q.takeFirst.yMin, r.takeLast.xMax, s.takeLast.yMax)
            p.unpersist(blockUntilUnpersisted)
            q.unpersist(blockUntilUnpersisted)
            r.unpersist(blockUntilUnpersisted)
            s.unpersist(blockUntilUnpersisted)
            if (partitionViaJava) {
              futures += new CreateKdTree().createFutureXminViaJava(pCollected, qCollected, temporary, 0, temporary.length - 1)
            } else {
              futures += new CreateKdTree().createFutureXminViaScala(pCollected, qCollected, temporary, 0, temporary.length - 1)
            }
            box
          }
          case YMIN => {
            val sCollected: Array[BoundingBox] = s.collect
            val pCollected: Array[BoundingBox] = p.collect
            val temporary = new Array[BoundingBox](pCollected.length)
            val node: BoundingBox = pCollected((pCollected.length - 1) >> 1)
            val box = new BoundingBox(node.id, s.takeFirst.xMin, p.takeFirst.yMin, q.takeLast.xMax, r.takeLast.yMax)
            p.unpersist(blockUntilUnpersisted)
            q.unpersist(blockUntilUnpersisted)
            r.unpersist(blockUntilUnpersisted)
            s.unpersist(blockUntilUnpersisted)
            if (partitionViaJava) {
              futures += new CreateKdTree().createFutureYminViaJava(sCollected, pCollected, temporary, 0, temporary.length - 1)
            } else {
              futures += new CreateKdTree().createFutureYminViaScala(sCollected, pCollected, temporary, 0, temporary.length - 1)
            }
            box
          }
          case XMAX => {
            val rCollected: Array[BoundingBox] = r.collect
            val sCollected: Array[BoundingBox] = s.collect
            val temporary = new Array[BoundingBox](rCollected.length)
            val node: BoundingBox = rCollected((rCollected.length - 1) >> 1)
            val box = BoundingBox(node.id, r.takeFirst.xMin, s.takeFirst.yMin, p.takeLast.xMax, q.takeLast.yMax)
            p.unpersist(blockUntilUnpersisted)
            q.unpersist(blockUntilUnpersisted)
            r.unpersist(blockUntilUnpersisted)
            s.unpersist(blockUntilUnpersisted)
            if (partitionViaJava) {
              futures += new CreateKdTree().createFutureXminViaJava(rCollected, sCollected, temporary, 0, temporary.length - 1)
            } else {
              futures += new CreateKdTree().createFutureXminViaScala(rCollected, sCollected, temporary, 0, temporary.length - 1)
            }
            box
          }
          case YMAX => {
            val qCollected: Array[BoundingBox] = q.collect
            val rCollected: Array[BoundingBox] = r.collect
            val temporary = new Array[BoundingBox](rCollected.length)
            val node: BoundingBox = rCollected((rCollected.length - 1) >> 1)
            val box = BoundingBox(node.id, q.takeFirst.xMin, r.takeFirst.yMin, s.takeLast.xMax, p.takeLast.yMax)
            p.unpersist(blockUntilUnpersisted)
            q.unpersist(blockUntilUnpersisted)
            r.unpersist(blockUntilUnpersisted)
            s.unpersist(blockUntilUnpersisted)
            if (partitionViaJava) {
              futures += new CreateKdTree().createFutureYminViaJava(qCollected, rCollected, temporary, 0, temporary.length - 1)
            } else {
              futures += new CreateKdTree().createFutureYminViaScala(qCollected, rCollected, temporary, 0, temporary.length - 1)
            }
            box
          }
          case _ => throw new RuntimeException(s"illegal permutation = $permutation")
        }
      }
    }

    // Sort the RDD[BoundingBox] in xMin, yMin, xMax and yMax then in id to avoid equality.
    // Persist the four sorted RDD[BoundingBox] for the call to the createKdTree function that will unpersist
    // these four RDD[BoundingBox] via side effect. If the RDD[BoundingBox] is used only to build the k-d tree,
    // it is possible to unpersist it immediately following the four sorts.  However, if the RDD[BoundingBox]
    // will be used subsequently to search the k-d tree, it would be better not to unpersist it following sort.
    val startTime = System.currentTimeMillis
    val pSorted = boundingBoxes.sortBy(bb => (bb.xMin, bb.id)).persist(persistenceLevel)
    val qSorted = boundingBoxes.sortBy(bb => (bb.yMin, bb.id)).persist(persistenceLevel)
    val rSorted = boundingBoxes.sortBy(bb => (bb.xMax, bb.id)).persist(persistenceLevel)
    val sSorted = boundingBoxes.sortBy(bb => (bb.yMax, bb.id)).persist(persistenceLevel)
    val endTimeSort = System.currentTimeMillis
    val sortTime = endTimeSort - startTime
    val root: BoundingBox = buildKdTree(pSorted, qSorted, rSorted, sSorted, numberOfBoxes, XMIN)
    val endTimeUpperTree = System.currentTimeMillis
    val upperTreeTime = endTimeUpperTree - endTimeSort

    // Prepend the elements from the ListBuffer[Future[List[KdNode]]]] to the List[KdNode] for aggregation to the RDD[KdNode].
    // Begin by converting the List[Future[List[KdNode]]] to a Future[List[List[KdNode]]].  See answer #35 of
    //
    // http://stackoverflow.com/questions/20012364/how-to-resolve-a-list-of-futures-in-scala
    //
    // See also answer #3 of
    //
    // http://stackoverflow.com/questions/20874186/scala-listfuture-to-futurelist-disregarding-failed-futures
    //
    // for a more sophisticated approach.
    val future: Future[List[List[KdNode]]] = Future.sequence(futures.toList)
    val result: List[List[KdNode]] = Await.result(future, Duration.Inf)
    val endTimeLowerTree = System.currentTimeMillis
    val lowerTreeTime = endTimeLowerTree - endTimeUpperTree
    result.foreach(l => l.foreach(k => prependToList(k)))
    val endTimeList = System.currentTimeMillis
    val listTime = endTimeList - endTimeLowerTree

    // Flush any remaining elements from the List[KdNode] to the RDD[KdNode].
    flushList

    // Count and report the number of KdNodes.
    val numberOfKdNodes: Long = kdNodeRdd.mapPartitions(iter => Array(iter.size).iterator, true).collect.sum

    // Return a tuple that comprises the root bounding box and an RDD[KdNode]
    (root, kdNodeRdd, numberOfKdNodes, sortTime, upperTreeTime, lowerTreeTime, listTime)
  }

}

