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

package prototypes.fst

import java.io.{InputStream, EOFException, Closeable}
import java.lang.{Long => JLong}

import org.apache.lucene.store.{DataInput, InputStreamDataInput}
import org.apache.lucene.util.IntsRef
import prototypes.core.MemoryMappedFile
import prototypes.core.IntsRef.IntsRefWrapper

/** A finite but possibly huge input, which we have to iterate
  * through to build our FST. Note that the elements passed to the
  * callback function may be the same references and *cannot* be safely
  * reused across calls. */
trait FstInput {
  def foreach[U](fun: (IntsRef, JLong) => U): Unit
}

/** Rather than feed each individual entry with a value of 1,
  * accumulate and send as soon as the new one comes along. */
trait AccumulatingFstInput extends FstInput {

  def foreach[U](fun: IntsRef => U): Unit

  def foreach[U](fun: (IntsRef, JLong) => U): Unit = {
    val previous: IntsRef = new IntsRef(10)
    var acc = 0L
    foreach { current: IntsRef =>
      if (acc == 0L) { // first entry
        require(previous.isEmpty)
        current.copyTo(previous)
      }
      if (previous == current) {
        acc += 1L
      } else {
        fun(previous, acc)
        previous.clear()
        current.copyTo(previous)
        acc = 1L
      }
    }
    if (acc != 0L) {
      fun(previous, acc)
    }
  }
}


class MappedAccInput(
    mmf: MemoryMappedFile
  , decoder: IntsRefDecoder
  , delimiter: Byte = 10
  ) extends AccumulatingFstInput with Closeable {
  def foreach[U](fun: IntsRef => U): Unit = {
    mmf.foreach {
      case b if b == delimiter =>
        fun(decoder.build())
        decoder.reset()
      case b =>
        decoder.next(b)
    }
  }
  def close(): Unit = {
    mmf.close()
  }
}

class VIntAccInput(
    in: InputStream
  , protected val scratch: IntsRef = new IntsRef(10)
  ) extends AccumulatingFstInput with VIntHelper with Closeable {
  protected val di = new InputStreamDataInput(in)
  def foreach[U](fun: IntsRef => U): Unit = {
    while (readVIntsRef(di, scratch)) {
      fun(scratch)
    }
  }
  def close(): Unit = {
    di.close()
  }
}

class VIntKeyValInput(
    in: InputStream
  , protected val scratch: IntsRef = new IntsRef(10)
  ) extends FstInput with VIntHelper with Closeable {
  protected val di = new InputStreamDataInput(in)
  def foreach[U](fun: (IntsRef, JLong) => U): Unit = {
    while (readVIntsRef(di, scratch)) {
      fun(scratch, di.readVLong())
    }
  }
  def close(): Unit = {
    di.close()
  }
}

trait VIntHelper {
  protected def readVIntsRef(in: DataInput, ref: IntsRef): Boolean = {
    ref.clear()
    val length = try { in.readVInt() } catch { case _: EOFException => 0 }
    for (i <- 0 until length) {
      ref.append(in.readVInt())
    }
    length != 0
  }
}

/** Straightforward implementation for comparison purposes. */
class SimpleIntsInput(file: java.io.File) extends FstInput {
  import prototypes.core.readLines
  def foreach[U](fun: (IntsRef, JLong) => U): Unit = {
    var previous: IntsRef = null
    var acc = 0L
    readLines(file).map(_.split(' ').map(_.toInt)).foreach {
      numbers =>
        val current = prototypes.core.IntsRef(numbers: _*)
        if (previous == null) {
          previous = current
        }
        if (current == previous) {
          acc += 1
        } else {
          fun(previous, acc)
          previous = current
          acc = 1
        }
    }
    if (previous != null) {
      fun(previous, acc)
    }
  }
}

class MappedKeyValInput(
    mmf: MemoryMappedFile
  , keyDecoder: IntsRefDecoder
  , valDecoder: PositiveLongDecoder
  , delim1: Byte = ','.toByte
  , delim2: Byte = '\n'.toByte
  ) extends FstInput with Closeable {
  def foreach[U](fun: (IntsRef, JLong) => U): Unit = {
    var key: IntsRef = null
    var inKey = true
    mmf.foreach {
      case b if b == delim1 && inKey =>
        key = keyDecoder.build()
        keyDecoder.reset()
        inKey = false
      case b if b == delim2 && !inKey =>
        require(key != null)
        fun(key, valDecoder.build())
        valDecoder.reset()
        inKey = true
      case b if inKey =>
        keyDecoder.next(b)
      case b =>
        valDecoder.next(b)
    }
  }
  def close(): Unit = {
    mmf.close()
  }
}
