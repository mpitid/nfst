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

import java.io.Closeable
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable

/** Memory mapped files larger than 2GB. */
class MemoryMappedFile(ic: FileChannel, size: Long) extends Closeable {
  import MemoryMappedFile.{MaxInt, MaxSize}
  require(size >= 0, "size cannot be < 0")
  require(size <= MaxSize, s"size cannot be > $MaxSize")

  protected val chunks = new mutable.ArraySeq[MappedByteBuffer](1 + (size / MaxInt).toInt)

  // Mapping chunks could be on demand, but the assumption here is that we scan through
  // the entire file at least once, and this is arguably the simplest way to implement that.
  // This begs the question, are mmaped files faster for a sequential scan?
  for (i <- chunks.indices) {
    chunks(i) = ic.map(FileChannel.MapMode.READ_ONLY, i * MaxInt, MaxInt.min(size - i * MaxInt))
  }

  def get(i: Long): Byte = {
    require(i >= 0)
    require(i < size)
    chunks(arrayIndex(i)).get(bufferIndex(i))
  }

  def foreach[U](fun: Byte => U): Unit = {
    foreach(0L, size)(fun)
  }

  def foreach[U](start: Long, end: Long)(fun: Byte => U): Unit = {
    require(start <= end)
    var i = start
    while (i < end) {
      fun(get(i))
      i += 1L
    }
  }

  protected def arrayIndex(i: Long): Int = {
    (i / MaxInt).toInt
  }
  protected def bufferIndex(i: Long): Int = {
    (i % MaxInt).toInt
  }

  def close(): Unit = {
    // make the mapped buffers eligible for collection even if this object persists.
    for (i <- chunks.indices) {
      chunks(i) = null
    }
    ic.close()
  }
}

object MemoryMappedFile {

  val MaxInt: Long = Int.MaxValue.toLong
  /** To keep things somewhat simpler, we cannot support files larger than this. */
  val MaxSize: Long = MaxInt * MaxInt - 1

  import java.nio.file.StandardOpenOption
  def apply(file: java.io.File): MemoryMappedFile = {
    new MemoryMappedFile(FileChannel.open(file.toPath, StandardOpenOption.READ), file.length())
  }
}
