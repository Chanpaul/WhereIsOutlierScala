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

import org.apache.log4j.Logger

/**
 * See answer #121 of https://stackoverflow.com/questions/2315912/scala-best-way-to-parse-command-line-parameters-cli
 */
object ParseArgs extends Serializable {

  val usage = s"Using default command-line options. Possible options are: [--input String] [--output String] [--partitions String] [--copies String]" +
    s" [--cutoff String] [--persist String] [--block String] [--kyro String] [--compress String] [--java String]"

  type OptionMap = Map[Symbol, Any]

  def parse(args: Array[String]): OptionMap = {
    // Supply empty args if args is null.
    val copyArgs = if (args == null) {
      Array[String]()
    } else {
      args
    }
    // Provide usage suggestion if no args were supplied.
    if (copyArgs.length == 0) {
      Logger.getRootLogger.warn(usage)
    }
    val arglist = copyArgs.toList

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--input" :: value :: tail =>
          nextOption(map ++ Map('input -> value), tail)
        case "--output" :: value :: tail =>
          nextOption(map ++ Map('output -> value), tail)
        case "--partitions" :: value :: tail =>
          nextOption(map ++ Map('partitions -> value), tail)
        case "--copies" :: value :: tail =>
          nextOption(map ++ Map('copies -> value), tail)
        case "--cutoff" :: value :: tail =>
          nextOption(map ++ Map('cutoff -> value), tail)
        case "--persist" :: value :: tail =>
          nextOption(map ++ Map('persist -> value), tail)
        case "--block" :: value :: tail =>
          nextOption(map ++ Map('block -> value), tail)
        case "--kryo" :: value :: tail =>
          nextOption(map ++ Map('kryo -> value), tail)
        case "--compress" :: value :: tail =>
          nextOption(map ++ Map('compress -> value), tail)
        case "--java" :: value :: tail =>
          nextOption(map ++ Map('java -> value), tail)
        case option :: tail => throw new IllegalArgumentException(s"unknown option: $option")
      }
    }

    nextOption(Map(), arglist)
  }
}
