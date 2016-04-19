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

package kdtree.build

import kdtree.box.BoundingBox
import kdtree.kdtree.{KdNode, CreateKdTree, SearchKdTree}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import kdtree.util.ParseArgs
import kdtree.util.ParseArgs._

/**
 * Build and search a two-dimensional k-d tree having a specified number of bounding boxes and report the elapsed times.
 */
object BuildAndSearchKdTreeTiming extends Serializable {

  /**
   * It does not appear necessary to provide any information to the main() method other than 'args'
   * all other information can be provided via spark-submit.  See
   *
   * https://spark.apache.org/docs/1.1.0/submitting-applications.html
   *
   * and in particular the example under "Launching Applications with spark-submit":
   *
   * ./bin/spark-submit \
   * --class <main-class>
   * --master <master-url> \
   * --deploy-mode <deploy-mode> \
   * --conf <key>=<value> \
   * ... # other options
   * <application-jar> \
   * [application-arguments]
   *
   * Also, the main() method must return type Unit in order to be executable from spark-submit.
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(s"Build and Search k-d Tree Timing")
    implicit val sc = new SparkContext(conf)

    elapsedTimes(args)
  }

  /**
   * This method allows BuildAndSearchKdTreeTimingTest to provide the appropriate SparkContext for a ScalaTest.
   */
  def elapsedTimes(args: Array[String])(implicit sc: SparkContext): Unit = {

    // Parse the arguments and supply defaults where necessary.
    val arguments: OptionMap = ParseArgs.parse(args)
    val input: String = arguments.get('input).getOrElse{s"src/test/resources/box/inputs/boundingBox.txt"}.toString
    val partitions: Int = arguments.get('partitions).getOrElse{s"1"}.toString.toInt
    val copies: Int = arguments.get('copies).getOrElse{s"1"}.toString.toInt
    val cutoff: Int = arguments.get('cutoff).getOrElse{s"32"}.toString.toInt
    val persist: String = arguments.get('persist).getOrElse{s"MEMORY_ONLY"}.toString
    val blockUntilUnpersisted: Boolean = arguments.get('block).getOrElse{s"false"}.toString.toBoolean
    val kryoSerializer: Boolean = arguments.get('kryo).getOrElse{s"false"}.toString.toBoolean
    val rddCompress: Boolean = arguments.get('compress).getOrElse{s"false"}.toString.toBoolean
    val partitionViaJava: Boolean = arguments.get('java).getOrElse(s"true").toString.toBoolean

    // Convert the --cache argument to a StorageLevel.
    val persistenceLevel = persist.toUpperCase match {
      case "DISK_ONLY" => StorageLevel.DISK_ONLY
      case "DISK_ONLY_2" => StorageLevel.DISK_ONLY_2
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_AND_DISK_2" => StorageLevel.MEMORY_AND_DISK_2
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_ONLY_2" => StorageLevel.MEMORY_ONLY_2
      case "MEMORY_ONLY_SER_2" => StorageLevel.MEMORY_ONLY_SER_2
      case "NONE" => StorageLevel.NONE
      case "OFF_HEAP" => StorageLevel.OFF_HEAP
      case _ => throw new RuntimeException(s"unsupported StorageLevel: $persist")
    }

    // Create a new SparkContext to specify the KryoSerializer if the --kryo argument is true or
    // to specify compression of serialized RDD partitions if the --compress argument is true.
    //
    // Stop the 'sc' SparkContext and create the new 'sparkContext' SparkContext. Specify this
    // new SparkContext explicitly via currying for the createKdTree() and searchKdTree() methods.
    // See http://spark.apache.org/docs/latest/configuration.html for configuration options.
    //
    // At present, if the --kryo argument is true, the SplitRddFunctions.getPartitionSizes() method
    // will not serialize via KryoSerializer but instead produces the following exception:
    //
    // org.apache.spark.SparkException: Task not serializable
    //
    // The exception appears to be related to the RDD.sortBy() method because if the boundingBoxes
    // RDD is not sorted in the KdTree.buildKdTree() method prior to calling the partitionByXmin()
    // method, the exception does not occur.
    val sparkContext = if (!kryoSerializer && !rddCompress) {
      sc
    } else {
      val sparkConf: SparkConf = sc.getConf
      if (rddCompress) {
        sparkConf.set(s"spark.rdd.compress", s"true")
      }
      if (kryoSerializer) {
        sparkConf.set(s"spark.serializer", s"org.apache.spark.serializer.KryoSerializer")
        sparkConf.set(s"spark.kryo.registrationRequired", s"true")
        sparkConf.set("spark.rdd.compress", "false") // If true, can save space at the cost of extra computation.
        sparkConf.registerKryoClasses(
          Array(
            classOf[KdNode],
            classOf[BoundingBox],
            classOf[Array[BoundingBox]], // For RDD[BoundingBox].collect
            classOf[scala.collection.mutable.WrappedArray.ofRef[Int]], // For RDD[BoundingBox].count and RDD[BoundingBox]mapPartitions
            classOf[Array[Int]], // For RDD[Int].collect
            classOf[Array[scala.Tuple3[_,_,_]]] // RDD[BoundingBox].sortBy -> Partitioner.RangePartitioner.sketch.collect
          )
        )
      }
      sc.stop
      new SparkContext(sparkConf)
    }

    // Aggregate the bounding boxes into this RDD[BoundingBox] that is empty initially.
    var boundingBoxRdd = sparkContext.parallelize(Array[BoundingBox]())
    var boundingBoxRddIsEmpty = true

    // Prepend the KdNodes to this List[KdNode].
    var boundingBoxList = List[BoundingBox]()

    // Prepend a BoundingBox to the List[BoundingBox] and if the List length equals or
    // exceeds the greater of partitions and cutoff, use the zipPartitions() method to
    // merge the List into the RDD[BoundingBox] such that the number of partitions is
    // preserved and all partitions have an equal number of elements (to within one
    // element) in order to balance the load during k-d tree building.
    def prependToList(box: BoundingBox) = {
      boundingBoxList = box :: boundingBoxList
      if (boundingBoxList.length >= Math.max(cutoff, partitions)) {
        boundingBoxRdd = if (boundingBoxRddIsEmpty) {
          boundingBoxRddIsEmpty = false
          sparkContext.parallelize(boundingBoxList, partitions)
        } else {
          boundingBoxRdd.zipPartitions(sparkContext.parallelize(boundingBoxList, partitions), preservesPartitioning=true)((iter, iter2) => iter++iter2)
        }.persist(persistenceLevel)
        boundingBoxList = List[BoundingBox]()
      }
    }

    // If KdNodes remain in the List[BoundingBox], use the zipPartitions() method to merge the
    // List into the RDD[BoundingBox] such that the number of partitions is preserved and all
    // partitions have an equal number of elements (to within one element) in order to
    // balance the load during k-d tree building.
    def flushList() = {
      if (boundingBoxList.nonEmpty) {
        boundingBoxRdd = if (boundingBoxRddIsEmpty) {
          boundingBoxRddIsEmpty = false
          sc.parallelize(boundingBoxList, partitions)
        } else {
          boundingBoxRdd.zipPartitions(sc.parallelize(boundingBoxList, partitions), preservesPartitioning=true)((iter, iter2) => iter++iter2)
        }.persist(persistenceLevel)
        boundingBoxList = List[BoundingBox]()
      }
    }

    // Parse the input file to create an RDD[BoundingBox] using the new SparkContext and persist that RDD.
    val boundingBoxes: RDD[BoundingBox] = sparkContext.textFile(input, 1)
      .map(_.split('\t'))
      .map(bs => bs.map(be => be.toLong))
      .zipWithIndex
      .map(bz => (bz._2.toLong, bz._1)) // Ensure that the index is Long and swap its position with the bounding box.
      .map(bz => BoundingBox.createBoundingBox(bz._1, bz._2(0), bz._2(1), bz._2(2), bz._2(3)))
      .persist(persistenceLevel)

    // Get the number of boxes in the Array[BoundingBox] for use in generating new Ids.
    val boundingBoxesArray = boundingBoxes.collect
    val numBoxes = boundingBoxesArray.length

    // Enlarge the extent of the Array{BoundingBox] by one unit for use in translating bounding boxes.
    val xMinSorted = boundingBoxesArray.sortBy(bb => bb.xMin)
    val yMinSorted = boundingBoxesArray.sortBy(bb => bb.yMin)
    val xMaxSorted = boundingBoxesArray.sortBy(bb => bb.xMax)
    val yMaxSorted = boundingBoxesArray.sortBy(bb => bb.yMax)
    val xMin = xMinSorted(0).xMin
    val yMin = yMinSorted(0).yMin
    val xMax = xMaxSorted(numBoxes - 1).xMax
    val yMax = yMaxSorted(numBoxes - 1).yMax
    val xExtent = xMax - xMin + 1
    val yExtent = yMax - yMin + 1

    // Create instances of the bounding boxes that are translated and assigned unique ids.  Record the elapsed time.
    val startTime = System.currentTimeMillis
    for (i <- 0 to copies - 1) {
      val yTranslate = i * yExtent
      for (j <- 0 to copies - 1) {
        val xTranslate = j * xExtent
        val idOffset = i * numBoxes * copies + j * numBoxes
        for (k <- 0 to numBoxes - 1) {
          prependToList(BoundingBox.translateBoundingBox(boundingBoxesArray(k), idOffset, xTranslate, yTranslate))
        }
      }
    }
    flushList
    val endTimeCopy = System.currentTimeMillis
    val copyTime = endTimeCopy - startTime

    // Build the k-d tree and record the elapsed time.
    val (root, kdNodes, numKdNodes, sortTime, upperTreeTime, lowerTreeTime, listTime) =
      CreateKdTree.createKdTree(boundingBoxRdd, partitions, cutoff, persistenceLevel, blockUntilUnpersisted, partitionViaJava)(sparkContext)
    val endTimeBuild = System.currentTimeMillis
    val buildTime = endTimeBuild - endTimeCopy

    // Search the k-d tree and record the elapsed time.
    val searchResult = SearchKdTree.searchKdTree(root, kdNodes, boundingBoxRdd, partitions, persistenceLevel)(sparkContext)
    val endTimeSearch = System.currentTimeMillis
    val searchTime = endTimeSearch - endTimeBuild
    val totalTime = endTimeSearch - startTime

    // Count the number of search results from the basic RDD[BoundingBox].
    val (baseRoot, baseKdNodes, baseNumKdNodes, baseSortTime, baseUpperTreeTime, baseLowerTreeTime, baseListTime) =
      CreateKdTree.createKdTree(boundingBoxes, partitions, cutoff, persistenceLevel, blockUntilUnpersisted, partitionViaJava)(sparkContext)
    val baseResult = SearchKdTree.searchKdTree(baseRoot, baseKdNodes, boundingBoxes, partitions, persistenceLevel)(sparkContext)
    val baseCount = baseResult.map(r => r._2.size).reduce{case (x, y) => x + y}

    // Count the number of search results from the instanced bounding boxes and verify that it is correct.
    val resultCount = searchResult.map(r => r._2.size).reduce{case (x, y) => x + y}
    assert(resultCount == copies * copies * baseCount, s"incorrect number of search results" +
      s"expected = ${copies * copies * baseCount}  actual = $resultCount")

    // Report the number of bounding boxes and the elapsed times.
    Logger.getRootLogger.warn(
      s"number of bounding boxes = $numKdNodes ," +
      s" copy time = $copyTime ms," +
      s" sort time = $sortTime ms," +
      s" upper tree time = $upperTreeTime ms," +
      s" lower tree time = $lowerTreeTime ms," +
      s" list time = $listTime ms," +
      s" build time = $buildTime ms," +
      s" search time = $searchTime ms," +
      s" total time = $totalTime ms"
    )

    // Shutdown.
    sc.stop
  }
}

