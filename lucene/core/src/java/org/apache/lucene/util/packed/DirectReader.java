/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util.packed;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link DirectWriter}
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = 100;
 *   IndexInput in = dir.openInput("packed", IOContext.DEFAULT);
 *   LongValues values = DirectReader.getInstance(in.randomAccessSlice(start, end), bitsPerValue);
 *   for (int i = 0; i &lt; numValues; i++) {
 *     long value = values.get(i);
 *   }
 * </pre>
 *
 * @see DirectWriter
 */
public final class DirectReader {

  /**
   * Retrieves an instance from the specified slice written decoding {@code bitsPerValue} for each
   * value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue) {
    return getInstance(slice, bitsPerValue, 0);
  }

  /**
   * Retrieves an instance from the specified {@code offset} of the given slice decoding {@code
   * bitsPerValue} for each value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue, long offset) {
    switch (bitsPerValue) {
      case 1:
        return index -> {
          try {
            int shift = (int) (index & 7);
            return (slice.readByte(offset + (index >>> 3)) >>> shift) & 0x1;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 2:
        return index -> {
          try {
            int shift = ((int) (index & 3)) << 1;
            return (slice.readByte(offset + (index >>> 2)) >>> shift) & 0x3;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 4:
        return index -> {
          try {
            int shift = (int) (index & 1) << 2;
            return (slice.readByte(offset + (index >>> 1)) >>> shift) & 0xF;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 8:
        return index -> {
          try {
            return slice.readByte(offset + index) & 0xFF;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 12:
        return index -> {
          try {
            long ofs = (index * 12) >>> 3;
            int shift = (int) (index & 1) << 2;
            return (slice.readShort(offset + ofs) >>> shift) & 0xFFF;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 16:
        return index -> {
          try {
            return slice.readShort(offset + (index << 1)) & 0xFFFF;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 20:
        return index -> {
          try {
            long ofs = (index * 20) >>> 3;
            int shift = (int) (index & 1) << 2;
            return (slice.readInt(offset + ofs) >>> shift) & 0xFFFFF;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 24:
        return index -> {
          try {
            return slice.readInt(offset + index * 3) & 0xFFFFFF;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 28:
        return index -> {
          try {
            long ofs = (index * 28) >>> 3;
            int shift = (int) (index & 1) << 2;
            return (slice.readInt(offset + ofs) >>> shift) & 0xFFFFFFF;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 32:
        return index -> {
          try {
            return slice.readInt(offset + (index << 2)) & 0xFFFFFFFFL;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 40:
        return index -> {
          try {
            return slice.readLong(offset + index * 5) & 0xFFFFFFFFFFL;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 48:
        return index -> {
          try {
            return slice.readLong(offset + index * 6) & 0xFFFFFFFFFFFFL;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 56:
        return index -> {
          try {
            return slice.readLong(offset + index * 7) & 0xFFFFFFFFFFFFFFL;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      case 64:
        return index -> {
          try {
            return slice.readLong(offset + (index << 3));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
      default:
        throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    }
  }

  private DirectReader() {
    // no instance
  }
}
