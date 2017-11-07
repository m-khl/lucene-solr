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
package org.apache.lucene.codecs.derivativeterms;

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

class PostingWriterDelegate extends PostingsWriterBase {
  private final PostingsWriterBase postingsWriter;

  PostingWriterDelegate(PostingsWriterBase postingsWriter) {
    this.postingsWriter = postingsWriter;
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    postingsWriter.init(termsOut, state);
  }

  FieldInfo getFieldInfo(){
    return ((PushPostingsWriterBase)postingsWriter).getField();
  }
  
  @Override
  public BlockTermState writeTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen)
      throws IOException {
    return postingsWriter.writeTerm(term, termsEnum, docsSeen);
  }

  @Override
  public void encodeTerm(long[] longs, DataOutput out, FieldInfo fieldInfo, BlockTermState state,
      boolean absolute) throws IOException {
    postingsWriter.encodeTerm(longs, out, fieldInfo, state, absolute);
  }

  @Override
  public int setField(FieldInfo fieldInfo) {
    return postingsWriter.setField(fieldInfo);
  }

  @Override
  public void close() throws IOException {
    postingsWriter.close();
  }
}