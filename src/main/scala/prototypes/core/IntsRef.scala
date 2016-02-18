// The MIT License (MIT)
//
// Copyright (c) 2016 Michael Pitidis
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package prototypes.core

import org.apache.lucene.util.ArrayUtil

object IntsRef {

  import org.apache.lucene.util.IntsRef

  def apply(ns: Int*): IntsRef = {
    val ref = new IntsRef(ns.length)
    ns.foreach(ref.append)
    ref
  }

  /** Expose some of the functionality of IntsRefBuilder as implicits. */
  implicit class IntsRefWrapper(val underlying: IntsRef) extends AnyVal {
    def apply(index: Int): Int = {
      underlying.ints(underlying.offset + index)
    }
    def update(index: Int, value: Int): Unit = {
      underlying.ints(underlying.offset + index) = value
    }
    def size(): Int = {
      underlying.length - underlying.offset
    }
    def isEmpty: Boolean = {
      underlying.length == underlying.offset
    }
    def nonEmpty: Boolean = {
      !isEmpty
    }
    def copy(): IntsRef = {
      copyTo(new IntsRef(size()))
    }
    def copyTo(to: IntsRef): IntsRef = {
      to.grow(size())
      foreach(to.append)
      to
    }
    def last: Int = {
      require(nonEmpty)
      underlying.ints(underlying.length-1)
    }
    def lastIndex: Int = {
      require(nonEmpty)
      underlying.length - underlying.offset
    }
    def grow(max: Int): IntsRef = {
      underlying.ints = ArrayUtil.grow(underlying.ints, max)
      underlying
    }
    def append(v: Int): IntsRef = {
      underlying.ints = ArrayUtil.grow(underlying.ints, underlying.length + 1)
      underlying.ints(underlying.length) = v
      underlying.length += 1
      underlying
    }
    def appendUnsafe(v: Int): IntsRef = {
      underlying.ints(underlying.length) = v
      underlying.length += 1
      underlying
    }
    def clear(): IntsRef = {
      underlying.length = underlying.offset
      underlying
    }
    def indices: Range = {
      underlying.offset.until(underlying.length)
    }
    def foreach[U](f: Int => U): Unit = {
      var i = underlying.offset
      while (i < underlying.length) {
        f(underlying.ints(i))
        i += 1
      }
    }
    def toSeq: Seq[Int] = {
      val arr = new Array[Int](underlying.length)
      var i = 0
      foreach { v =>
        arr(i) = v
        i += 1
      }
      arr.toSeq
    }
  }
}

