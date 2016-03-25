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

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

import ByteScanner.DefaultBufferSize

trait ByteScanner {
  require(bufferSize > 0)
  def bufferSize: Int
  def foreach[U](fun: Byte => U): Unit
}

object ByteScanner {
  val DefaultBufferSize: Int = 16 * 1024
}

trait ByteBufferScanner extends ByteScanner {
  /** The buffer *cannot* be safely reused. */
  def foreachBuffer[U](fun: ByteBuffer => U): Unit

  def foreach[U](fun: Byte => U): Unit = {
    foreachBuffer { bb: ByteBuffer =>
      while (bb.hasRemaining) {
        fun(bb.get())
      }
    }
  }
}

class RAFByteScanner(
    file: java.io.File
  , val bufferSize: Int = DefaultBufferSize
  ) extends ByteScanner {
  import java.io.RandomAccessFile
  def foreach[U](fun: Byte => U): Unit = {
    val buf = new Array[Byte](bufferSize)
    val raf = new RandomAccessFile(file, "r")
    try {
      var bytes = raf.read(buf)
      while (bytes != -1) {
        var i = 0
        while (i < bytes) {
          fun(buf(i))
          i += 1
        }
        bytes = raf.read(buf)
      }
    } finally {
      raf.close()
    }
  }
}

class NIOByteScanner(
    file: java.io.File
  , val bufferSize: Int = DefaultBufferSize
  , val direct: Boolean = false
  ) extends ByteBufferScanner {
  def foreachBuffer[U](fun: ByteBuffer => U): Unit = {
    val buf = if (direct) ByteBuffer.allocateDirect(bufferSize) else ByteBuffer.allocate(bufferSize)
    val in = FileChannel.open(file.toPath, StandardOpenOption.READ)
    try {
      while (in.read(buf) != -1) {
        buf.flip()
        fun(buf)
        buf.clear()
      }
    } finally {
      in.close()
    }
  }
}

class MMapByteScanner(
    file: java.io.File
  , val bufferSize: Int = Int.MaxValue
  ) extends ByteBufferScanner {
  def foreachBuffer[U](fun: ByteBuffer => U): Unit = {
    var offset = 0L
    val length = file.length()
    val in = FileChannel.open(file.toPath, StandardOpenOption.READ)
    try {
      while (offset != length) {
        val buf = in.map(MapMode.READ_ONLY, offset, (length - offset).min(bufferSize))
        fun(buf)
        offset += buf.capacity()
      }
    } finally {
      in.close()
    }
  }
}
