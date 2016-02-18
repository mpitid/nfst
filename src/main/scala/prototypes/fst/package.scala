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

package prototypes

import org.apache.lucene.util.IntsRef
import org.apache.lucene.util.fst.FST

package object fst {

  type IntsRefDecoder = ByteDecoder[IntsRef]

  implicit class FSTWrapper[T](val fst: FST[T]) extends AnyVal {
    import prototypes.core.IntsRef.IntsRefWrapper
    /** Looking up keys is a complex operation so abstract it away. */
    def get(key: IntsRef): Option[T] = {
      require(fst.inputType == FST.INPUT_TYPE.BYTE4)
      val reader = fst.getBytesReader
      val arc = fst.getFirstArc(new FST.Arc[T])
      var output = fst.outputs.getNoOutput
      for (v <- key) {
        if (fst.findTargetArc(v, arc, arc, reader) == null) {
          return None
        }
        output = fst.outputs.add(output, arc.output)
      }
      if (arc.isFinal) {
        Some(fst.outputs.add(output, arc.nextFinalOutput))
      } else {
        None
      }
    }
  }
}
