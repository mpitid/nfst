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

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}

import org.apache.lucene.util.IntsRef
import org.apache.lucene.util.fst.{FST, PositiveIntOutputs}
import org.rogach.scallop.{ScallopConf, Subcommand}
import prototypes.core.IntsRef.IntsRefWrapper
import prototypes.core._

object cli {
  def main(args: Array[String]): Unit = {
    val opts = new Options(args)
    opts.subcommand match {
      case Some(c @ opts.map) =>
        val delim = c.delimiter().toByte
        val inputFile = c.input().file
        val input: FstInput =
          (c.format(): @unchecked) match {
            case f @ ("byte" | "text") =>
              val mmf = MemoryMappedFile(inputFile)
              val dec = if (f == "byte") new RawByteDecoder() else new UTF8Decoder()
              if (c.values()) {
                new MappedKeyValInput(mmf, dec, new PositiveLongDecoder, delim)
              } else {
                new MappedAccInput(mmf, dec)
              }
            case "ints" =>
              val dec = new PositiveIntsDecoder(exact = !c.unsafe())
              if (c.values()) {
                new MappedKeyValInput(MemoryMappedFile(inputFile), dec, new PositiveLongDecoder(exact = !c.unsafe()), delim)
              } else {
                if (c.simpleInts()) {
                  new SimpleIntsInput(inputFile)
                } else {
                  new MappedAccInput(MemoryMappedFile(inputFile), dec)
                }
              }
            case "vint" =>
              val is = new BufferedInputStream(new FileInputStream(inputFile))
              if (c.values()) {
                new VIntKeyValInput(is)
              } else {
                new VIntAccInput(is)
              }
        }
        val outputFile = c.output().file
        val output: Output =
          (c.outputFormat(): @unchecked) match {
            case "fst" =>
              new FstOutput(outputFile)
            case "ints" =>
              new IntOutput(new BufferedOutputStream(new FileOutputStream(outputFile)), !c.unrolled(), delim)
            case "vint" =>
              new VIntOutput(new BufferedOutputStream(new FileOutputStream(outputFile)), !c.unrolled())
            case "noop" =>
              new NoopOutput
          }
        val validate = !c.noCheck()
        val previous = new IntsRef(10)
        input.foreach {
          case (k, v) =>
            if (validate) {
              if (previous.nonEmpty) {
                if (k.compareTo(previous) < 0) {
                  System.err.println(s"input is not in correct order: key ${k.toSeq.mkString(" ")} precedes ${previous.toSeq.mkString(" ")}")
                  System.exit(1)
                }
                previous.clear()
                k.copyTo(previous)
              } else {
                k.copyTo(previous)
              }
            }
            output.write(k, v)
        }
        output.close()
      case Some(c @ opts.get) =>
        import prototypes.fst.FSTWrapper
        val dec =
          (c.format(): @unchecked) match {
            case "byte" => new RawByteDecoder()
            case "text" => new UTF8Decoder()
            case "ints" | "vint" => new PositiveIntsDecoder()
          }
        val fst = FST.read(c.input().file.toPath, PositiveIntOutputs.getSingleton)
        for (key <- c.keys()) {
          val v = fst.get(dec.apply(key.getBytes("UTF-8")))
          println(s"$key ${v.orNull}")
        }
      case other =>
        System.err.println(s"unexpected subcommand $other")
        System.exit(2)
    }
  }

  class Options(args: Seq[String]) extends ScallopConf(args) {
    version("fst 1.0.0")
    val formats = Seq(
      "byte"
    , "text"
    , "ints"
    , "vint"
    )
    val outputFormats = Seq(
      "fst"
    , "ints"
    , "vint"
    , "noop"
    )
    val map = new Subcommand("map") {
      val format = opt[String](short = 'f', default = formats.headOption,
        descr = s"input file format, one of ${formats.mkString(", ")}")
      val simpleInts = opt[Boolean](noshort = true)
      val values = opt[Boolean](short = 'v',
        descr = "read values from input file instead of counting consecutive entries")
      val delimiter = opt[Int](short = 'd', default = Some(','.toInt), validate = d => d >= 0 && d <= 255,
        descr = "raw byte value for key-value delimiter when reading value")
      val noCheck = opt[Boolean](short = 'n',
        descr = "do not check if input is sorted")
      val outputFormat = opt[String](short = 'o', default = outputFormats.headOption,
        descr = s"output file format, one of ${formats.mkString(", ")}")
      val unrolled = opt[Boolean](short = 'u',
        descr = "for output formats other than FST repeat keys instead of storing counts as values")
      val unsafe = opt[Boolean](noshort = true,
        descr = "do not check for overflow when parsing integer or long values")
      val input = trailArg[String]("input", required = true,
        descr = "input file to read data from")
      val output = trailArg[String]("output", required = true,
        descr = "output file to store FST to")
      val help = opt[Boolean](short = 'h', descr = "show help message")
    }
    val get = new Subcommand("get") {
      val format = opt[String](short = 'f', default = formats.headOption,
        descr = s"input file format used when generating the FST, one of ${formats.mkString(", ")}")
      val input = trailArg[String]("input", required = true,
        descr = "input FST file")
      val keys = trailArg[List[String]]("keys",
        descr = "UTF-8 encoded keys to look up")
      val help = opt[Boolean](short = 'h', descr = "show help message")
    }
    val help = opt[Boolean](short = 'h', descr = "show help message")
  }
}
