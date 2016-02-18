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
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, MustMatchers}
import prototypes.core.IntsRef.IntsRefWrapper


class ByteDecoderSpec extends FlatSpec with MustMatchers with PropertyChecks {

  "The UTF8 decoder" should "support multi-byte sequences" in {
    val dec = new UTF8Decoder()
    val a = 0xC2
    val b = 0xAB
    val res = dec.reset()
                 .next(a.toByte)
                 .next(b.toByte)
                 .build().toSeq
    res must equal(Seq(((a & 0x7F) << 8) | b))
  }

  val longDec = new PositiveLongDecoder()
  val delimiter = ' '.toByte
  val intsDec = new PositiveIntsDecoder(delimiter = delimiter)

  val utf8Dec = new UTF8Decoder()
  val bwDec = new RawByteDecoder()

  val positiveInts = Gen.choose(0, Int.MaxValue)

  "The integer decoder" should "support arbitrary sequences of positive integers" in {
    forAll(Gen.nonEmptyListOf(positiveInts)) { ints =>
      val bytes = ints.map(_.toString).mkString(delimiter.toChar.toString).getBytes("UTF-8")
      Encoders.ints(intsDec.apply(bytes), delimiter).toSeq must equal(bytes)
      Encoders.utf8(utf8Dec.apply(bytes)).toSeq must equal(bytes.toSeq)
      Encoders.rawBytes(bwDec.apply(bytes)).toSeq must equal(bytes.toSeq)
    }
  }

  it should "throw errors on non-numeric input" in {
    forAll(Gen.nonEmptyListOf(Gen.alphaChar)) { l =>
      intercept[NumberFormatException] {
        intsDec.apply(l.mkString.getBytes("UTF-8"))
      }
    }
  }

  it should "optionally throw errors on overflow" in {
    val dec1 = new PositiveIntsDecoder()
    val dec2 = new PositiveIntsDecoder(exact = false)
    forAll { l: Long =>
      whenever(l >= Int.MaxValue) {
        val bytes = l.toString.getBytes("UTF-8")
        intercept[ArithmeticException] {
          dec1.apply(bytes)
        }
        dec2.apply(bytes)
      }
    }
  }

  "The UTF8 and raw byte decoders" should "support arbitrary UTF-8 strings" in {
    forAll { s: String =>
      val bytes = s.getBytes("UTF-8")
      Encoders.utf8(utf8Dec.apply(bytes)).toSeq must equal(bytes.toSeq)
      Encoders.rawBytes(bwDec.apply(bytes)).toSeq must equal(bytes.toSeq)
    }
  }

  "The long decoder" should "support arbitrary positive longs" in {
    forAll(Gen.choose(0L, Long.MaxValue)) { l: Long =>
      l must equal(longDec.apply(l.toString.getBytes("UTF-8")))
    }
  }

  val bigInts = Gen.choose(2L, Long.MaxValue).map(_ * BigInt(Long.MaxValue))

  it should "optionally throw errors on overflow" in {
    forAll(bigInts) { b =>
      val dec2 = new PositiveLongDecoder(exact = false)
      intercept[ArithmeticException] {
        longDec.apply(b.toString.getBytes("UTF-8"))
      }
      dec2.apply(b.toString.getBytes("UTF-8"))
    }
  }
}

object Encoders {
  def rawBytes(ref: IntsRef): Array[Byte] = {
    ref.toSeq.map(_.toByte).toArray
  }

  import scala.collection.mutable.ListBuffer
  def utf8(ref: IntsRef): Array[Byte] = {
    val buf = new ListBuffer[Byte]()
    ref.foreach { i =>
      val a = i >> 24
      val b = (i >> 16) & 0xFF
      val c = (i >> 8) & 0xFF
      val d = i & 0xFF
      for (byte <- Seq(a, b, c); if byte != 0) {
        buf.append((byte | 0x80).toByte)
      }
      buf.append(d.toByte)
    }
    buf.toArray
  }

  def ints(ref: IntsRef, delim: Byte): Array[Byte] = {
    ref.toSeq.map(_.toString).mkString(delim.toChar.toString).getBytes("UTF-8")
  }
}
