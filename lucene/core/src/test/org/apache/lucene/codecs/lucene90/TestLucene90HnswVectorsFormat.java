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
package org.apache.lucene.codecs.lucene90;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.codecs.lucene90.Lucene90HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene90.Lucene90HnswVectorsFormat.DEFAULT_MAX_CONN;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene90HnswVectorsFormat extends BaseKnnVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return TestUtil.getDefaultCodec();
  }

  public void testToString() {
    int maxConn = randomIntBetween(DEFAULT_MAX_CONN - 10, DEFAULT_MAX_CONN + 10);
    int beamWidth = randomIntBetween(DEFAULT_BEAM_WIDTH - 50, DEFAULT_BEAM_WIDTH + 50);
    Codec customCodec =
        new Lucene90Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new Lucene90HnswVectorsFormat(maxConn, beamWidth);
          }
        };
    String expectedString =
        "Lucene90HnswVectorsFormat(name = Lucene90HnswVectorsFormat, maxConn = "
            + maxConn
            + ", beamWidth="
            + beamWidth
            + ")";
    assert (((Lucene90Codec) customCodec)
        .getKnnVectorsFormatForField("bogus_field")
        .toString()
        .equals(expectedString));
  }
}
