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

package kdtree.util

import org.apache.spark.SparkContext
import kdtree.util.ParseArgs.OptionMap

/**
 * It does not appear necessary to provide any information to the main() function other than 'args'
 * and the implicit SparkContext parameter that is provided by ScalaTest.  However, this implicit
 * parameter is incompatible with execution via the spark-submit script but is OK for a ScalaTest.
 */
object CheckArgs extends Serializable {

  // Although the sc: SparkContext parameter is not used here, it is included to check
  // a correct call from CheckArgsTest.
  def main(args: Array[String])(implicit sc: SparkContext): Unit = {
    val arguments: OptionMap = ParseArgs.parse(args)
    val input: String = arguments.get('input).getOrElse{throw new RuntimeException(s"no --input specified")}.toString
    val output: String = arguments.get('output).getOrElse{throw new RuntimeException(s"no --output specified")}.toString
    val partitions: String = arguments.get('partitions).getOrElse{throw new RuntimeException(s"no --partitions specified")}.toString
    val copies: String = arguments.get('copies).getOrElse{throw new RuntimeException(s"no --copies specified")}.toString
    val cutoff: String = arguments.get('cutoff).getOrElse{throw new RuntimeException(s"no --cutoff specified")}.toString
    val persist: String = arguments.get('persist).getOrElse{throw new RuntimeException(s"no --persist specified")}.toString
    val block: String = arguments.get('block).getOrElse{throw new RuntimeException(s"no --block specified")}.toString
    val kryo: String = arguments.get('kryo).getOrElse{throw new RuntimeException(s"no --kryo specified")}.toString
    val compress: String = arguments.get('compress).getOrElse{throw new RuntimeException(s"no --compress specified")}.toString
    val java: String = arguments.get('java).getOrElse{throw new RuntimeException(s"no --java specified")}.toString
    println(s"number of arguments = " + arguments.toList.length)
    println(s"input = $input")
    println(s"output = $output")
    println(s"partitions = $partitions")
    println(s"copies = $copies")
    println(s"cutoff = $cutoff")
    println(s"persist = $persist")
    println(s"block = $block")
    println(s"kryo = $kryo")
    println(s"compress = $compress")
    println(s"java = $java")
  }

}
