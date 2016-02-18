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

import org.apache.lucene.util.IntsRef
import prototypes.core.IntsRef.IntsRefWrapper

/** An incremental decoder from bytes to arbitrary values. */
trait ByteDecoder[T] {
  def reset(): this.type
  def next(byte: Byte): this.type
  def build(): T
  def apply(bytes: Array[Byte]): T = {
    bytes.foldLeft(reset())(_ next _).build()
  }
}

/** Decode a single positive Long. */
class PositiveLongDecoder(exact: Boolean = true) extends ByteDecoder[Long] {
  protected var acc = 0L
  def reset() = {
    acc = 0
    this
  }
  def next(byte: Byte) = {
    if (byte < 48 || byte > 57) {
      throw new NumberFormatException(s"value out of range $byte")
    }
    if (exact) { // optional because of speed and Java 8 dependency
      acc = Math.addExact(Math.multiplyExact(10L, acc), byte - 48)
    } else {
      acc = acc * 10L + (byte - 48)
    }
    this
  }
  def build() = {
    acc
  }
}

/** Decode bytes into a sequence of integers. */
class RawByteDecoder(scratch: IntsRef = new IntsRef(10)) extends IntsRefDecoder {
  def reset() = {
    scratch.clear()
    this
  }
  def next(byte: Byte) = {
    scratch.append(byte & 0xFF)
    this
  }
  def build(): IntsRef = {
    scratch
  }
}

/** Decode a sequence of ASCII positive integers. */
class PositiveIntsDecoder(delimiter: Byte = ' '.toByte, exact: Boolean = true, scratch: IntsRef = new IntsRef(10)) extends IntsRefDecoder {
  protected var acc: Int = 0
  protected var pending: Boolean = false
  def reset() = {
    scratch.clear()
    acc = 0
    pending = false
    this
  }
  def next(byte: Byte) = {
    if (byte >= 48 && byte <= 57) {
      pending = true
      if (exact) { // optional because of speed and Java 8 dependency
        acc = Math.addExact(Math.multiplyExact(10, acc), byte - 48)
      } else {
        acc = 10 * acc + (byte - 48)
      }
    } else if (byte == delimiter) {
      if (pending) {
        pending = false
        scratch.append(acc)
        acc = 0
      }
    } else {
      throw new NumberFormatException(s"unexpected byte: $byte ${(byte & 0xFF).toBinaryString}")
    }
    this
  }
  def build(): IntsRef = {
    if (pending) {
      scratch.append(acc)
    }
    scratch
  }
}

/** Decode UTF-8 strings into a sequence of integers.
  *
  * Each character is encoded as a single integer with the following scheme:
  * 1 bytes: MSB 0000 0000 0000 byte LSB
  * 2 bytes:     0000 0000 byt1 byt2
  * 3 bytes:     0000 byt1 byt2 byt3
  * 4 bytes:     byt1 byt2 byt3 byt4
  *
  * This ensures the sort order is the same as UNIX's sort with LC_ALL=C.
  * To work around 31-bit integers, the MSB of multi-byte characters is set to 0.
  */
class UTF8Decoder(scratch: IntsRef = new IntsRef(10)) extends IntsRefDecoder {
  protected var acc: Int = 0
  protected var expecting: Int = 0
  protected var entry = 1L
  protected var offset = 1L
  protected var pending = false
  def reset() = {
    scratch.clear()
    acc = 0
    expecting = 0
    pending = false
    this
  }
  def next(byte: Byte) = {
    if (expecting == 0) {
      if (pending) {
        scratch.append(acc)
      }
      expecting = expectedBytes(byte)
      // Because of the shorter integers, we have to convert the leading
      // 1 to a 0, otherwise 4-byte sequences will result in negative integers.
      acc = (byte & 0x7F) << (8 * expecting)
    } else {
      // validate this is a multi-byte (starts with 10)
      require((byte & 0xC0) == 0x80,
        s"invalid UTF-8 continuation byte on entry $entry offset $offset expecting $expecting: $byte ${(byte&0xFF).toBinaryString}")
      expecting -= 1
      acc |= (byte & 0xFF) << (8 * expecting)
    }
    pending = true
    offset += 1
    this
  }
  // https://tools.ietf.org/html/rfc3629#page-4
  // 0000 0000-0000 007F | 0xxxxxxx
  // 0000 0080-0000 07FF | 110xxxxx 10xxxxxx
  // 0000 0800-0000 FFFF | 1110xxxx 10xxxxxx 10xxxxxx
  // 0001 0000-0010 FFFF | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
  protected def expectedBytes(b: Byte): Int = {
    if ((b & 0x80) == 0) {
      0
    } else if ((b & 0xE0) == 0xC0) {
      1
    } else if ((b & 0xF0) == 0xE0) {
      2
    } else if ((b & 0xF8) == 0xF0) {
      3
    } else {
      throw new IllegalArgumentException(s"invalid UTF-8 start byte on entry $entry offset $offset: $b ${(b&0xFF).toBinaryString}")
    }
  }
  def build(): IntsRef = {
    if (pending) {
      pending = false
      scratch.append(acc)
    }
    offset = 1
    entry += 1
    scratch
  }
}
