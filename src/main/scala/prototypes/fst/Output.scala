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

import java.io.{Closeable, OutputStream}
import java.lang.{Long => JLong}

import org.apache.lucene.store.OutputStreamDataOutput
import org.apache.lucene.util.IntsRef
import prototypes.core.IntsRef.IntsRefWrapper

trait Output extends Closeable {
  def write(key: IntsRef, value: JLong): Unit
  def close(): Unit
}

class FstOutput(file: java.io.File) extends Output {
  import org.apache.lucene.util.fst.{Builder, FST, PositiveIntOutputs}
  protected val builder = new Builder(FST.INPUT_TYPE.BYTE4, PositiveIntOutputs.getSingleton)
  def write(key: IntsRef, value: JLong): Unit = {
    builder.add(key, value)
  }
  def close(): Unit = {
    builder.finish().save(file.toPath)
  }
}

class IntOutput(
    os: OutputStream
  , compact: Boolean = true
  , valDelim: Byte = ','.toByte
  , intDelim: Byte = ' '.toByte
  , entryDelim: Byte = '\n'.toByte
  ) extends Output {
  def write(ref: IntsRef, value: JLong): Unit = {
    require(ref.nonEmpty)
    val bytes = ref.toSeq.mkString(intDelim.toChar.toString).getBytes("UTF-8")
    os.write(bytes)
    if (compact) {
      os.write(valDelim)
      os.write(value.toString.getBytes("UTF-8"))
      os.write(entryDelim)
    } else {
      os.write(entryDelim)
      var i: Long = value
      while (i > 1) {
        os.write(bytes)
        os.write(entryDelim)
        i -= 1
      }
    }
  }
  def close(): Unit = {
    os.close()
  }
}

class VIntOutput(os: OutputStream, compact: Boolean = true) extends Output {
  protected val oso = new OutputStreamDataOutput(os)
  def write(ref: IntsRef, value: JLong): Unit = {
    require(ref.nonEmpty)
    oso.writeVInt(ref.length)
    ref.foreach(oso.writeVInt)
    if (compact) {
      oso.writeVLong(value)
    } else {
      var i: Long = value
      while (i > 1L) {
        ref.foreach(oso.writeVInt)
        i -= 1L
      }
    }
  }
  def close(): Unit = {
    oso.close()
  }
}
