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

package kdtree.box

import scala.math.Ordering.Implicits._

object BoundingBox extends Serializable {

  // Translate an existing bounding box to create a new bounding box.
  def translateBoundingBox(bb: BoundingBox, id: Long, x: Long, y: Long) = {

    createBoundingBox(bb.id + id, bb.xMin + x, bb.yMin + y, bb.xMax + x, bb.yMax + y)
  }

  // Check bounding box parameters before creating a new bounding box.
  def createBoundingBox(name: Long, x_min: Long, y_min: Long, x_max: Long, y_max: Long) = {

    if (x_min >= x_max || y_min >= y_max) {
      throw new IllegalArgumentException(s"illegal bounding box dimensions: xMin = $x_min  yMin = $y_min  xMax = $x_max  yMax = $y_max")
    }
    new BoundingBox(name, x_min, y_min, x_max, y_max)
  }

  // The following functions are required to pass BoundingBox functions as arguments to the buildKdTree function.
  def xMinIsLess(bb: BoundingBox, node: BoundingBox): Boolean = { bb.xMinLessThan(node) }

  def xMinIsMore(bb: BoundingBox, node: BoundingBox): Boolean = { bb.xMinMoreThan(node) }

  def yMinIsLess(bb: BoundingBox, node: BoundingBox): Boolean = { bb.yMinLessThan(node) }

  def yMinIsMore(bb: BoundingBox, node: BoundingBox): Boolean = { bb.yMinMoreThan(node) }

  def xMaxIsLess(bb: BoundingBox, node: BoundingBox): Boolean = { bb.xMaxLessThan(node) }

  def xMaxIsMore(bb: BoundingBox, node: BoundingBox): Boolean = { bb.xMaxMoreThan(node) }

  def yMaxIsLess(bb: BoundingBox, node: BoundingBox): Boolean = { bb.yMaxLessThan(node) }

  def yMaxIsMore(bb: BoundingBox, node: BoundingBox): Boolean = { bb.yMaxMoreThan(node) }

}

case class BoundingBox(val id: Long,
                       val xMin: Long,
                       val yMin: Long,
                       val xMax: Long,
                       val yMax: Long) extends Serializable {

  // Intersect the bounding boxes only without examining the ids.
  def intersectsRegion(reg: BoundingBox): Boolean = {
    !(xMax < reg.xMin || yMax < reg.yMin || reg.xMax < xMin || reg.yMax < yMin)
  }

  // Intersect the bounding boxes only if their ids don't match.
  def intersectsBox(bb: BoundingBox): Boolean = {
    (id != bb.id) && !(xMax < bb.xMin || yMax < bb.yMin || bb.xMax < xMin || bb.yMax < yMin)
  }

  // Compare super-keys for xMin, yMin, xMax or yMax together with id.
  def xMinLessThan(bb: BoundingBox): Boolean = { (xMin, id) < (bb.xMin, bb.id) }

  def xMinMoreThan(bb: BoundingBox): Boolean = { (xMin, id) > (bb.xMin, bb.id) }

  def yMinLessThan(bb: BoundingBox): Boolean = { (yMin, id) < (bb.yMin, bb.id) }

  def yMinMoreThan(bb: BoundingBox): Boolean = { (yMin, id) > (bb.yMin, bb.id) }

  def xMaxLessThan(bb: BoundingBox): Boolean = { (xMax, id) < (bb.xMax, bb.id) }

  def xMaxMoreThan(bb: BoundingBox): Boolean = { (xMax, id) > (bb.xMax, bb.id) }

  def yMaxLessThan(bb: BoundingBox): Boolean = { (yMax, id) < (bb.yMax, bb.id) }

  def yMaxMoreThan(bb: BoundingBox): Boolean = { (yMax, id) > (bb.yMax, bb.id) }

  // Provide accessor methods for Java because Java can't see the auto-generated accessors.
  def getXmin(): Long = xMin

  def getYmin(): Long = yMin

  def getXmax(): Long = xMax

  def getYmax(): Long = yMax

  def getId(): Long = id

  override def toString() = {
    s"id:$id xMin:$xMin yMin:$yMin xMax:$xMax yMax:$yMax"
  }

}
