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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader.FilterFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

abstract class InjectingBlockTreeTermsWriter extends BlockTreeTermsWriter {
  
  final private Map<String,FieldData> termsByField;
  
  InjectingBlockTreeTermsWriter(SegmentWriteState state, PostingsWriterBase postingsWriter,
                      Map<String,FieldData> termsByField ) throws IOException {
    super(state, postingsWriter, BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
        BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
    this.termsByField = termsByField;
  }

  Fields decorate(Fields input) {
    return new FilterFields(input) {
      @Override
      public Terms terms(String field) throws IOException {
        final String originalFieldName = originalFieldName(field);
        if (originalFieldName!=null) {
          final FieldData source = termsByField.get(originalFieldName);
          final Map<BytesRef,BlockTermState> derivedTermsForCurrentField = deriveTerms(source);
          return new Terms() {
            
            @Override
            public long size() throws IOException {
              return derivedTermsForCurrentField.size();
            }
            
            @Override
            public TermsEnum iterator() throws IOException {
              return new BlockStateTermsEnum(derivedTermsForCurrentField, source.docsSeen);
            }
            
            @Override
            public boolean hasPositions() {
              return false;
            }
            
            @Override
            public boolean hasPayloads() {
              return false;
            }
            
            @Override
            public boolean hasOffsets() {
              return false;
            }
            
            @Override
            public boolean hasFreqs() {
              return false;
            }
            
            @Override
            public long getSumTotalTermFreq() throws IOException {
              return 0;
            }
            
            @Override
            public long getSumDocFreq() throws IOException {
              return 0;
            }
            
            @Override
            public int getDocCount() throws IOException {
              return 0;
            }
          };
        } else {
          return super.terms(field);
        }
      }
    };
  }

  protected abstract String originalFieldName(String field) ;

  @Override
  public void write(Fields fields) throws IOException {
    super.write(decorate(fields));
  }

  @Override
  protected TermsWriter createTermsWriter(final FieldInfo fieldInfo) {
    final String originalFieldName = originalFieldName(fieldInfo.name);
    if (originalFieldName != null) {
      return new TermsWriter(fieldInfo) {
        private FixedBitSet fieldDocs;

        @Override
        protected BlockTermState writePosting(BytesRef text, TermsEnum termsEnum) throws IOException {
          // remember fields' docs which we need when terms is over
          InjectingBlockTreeTermsWriter.BlockStateTermsEnum states = (InjectingBlockTreeTermsWriter.BlockStateTermsEnum) termsEnum;
          fieldDocs = states.getFieldDocs();
          // don't write anything, just retrieve FP
          return states.termState();
        };

        @Override
        public void finish() throws IOException {

          if (fieldDocs != null) {
            docsSeen.or(fieldDocs);
          }
          super.finish();
        }
      };
    } else {
      return super.createTermsWriter(fieldInfo);
    }
  }


  protected abstract Map<BytesRef,BlockTermState> deriveTerms(FieldData stateByTerms) throws IOException;

  private static final class BlockStateTermsEnum extends TermsEnum {
  
    private final Iterator<Entry<BytesRef,BlockTermState>> sourceIter;
    private final FixedBitSet fieldDocs;
  
    private BlockTermState termState;
    private BytesRef term;
  
    private BlockStateTermsEnum(Map<BytesRef,BlockTermState> derivedTermsForCurrentField, FixedBitSet fieldDocs) {
      this.sourceIter = derivedTermsForCurrentField.entrySet().iterator();
      this.fieldDocs = fieldDocs;
    }
  
    @Override
    public BytesRef next() throws IOException {
  
      if(!sourceIter.hasNext()) {
        return null;
      }
      final Entry<BytesRef,BlockTermState> next = sourceIter.next();
      termState = next.getValue();
      term = next.getKey();
     // System.out.println(term + " " + termState);
      return term;
    }
  
    /** @returns state associated with the last returned term */
    public BlockTermState termState() {
      return termState;
    }
  
    /** @returns docs having any field values */
    public FixedBitSet getFieldDocs() {
      return fieldDocs;
    }
  
    @Override
    public long totalTermFreq() throws IOException {
      return 0;
    }
  
    @Override
    public BytesRef term() throws IOException {
      return term;
    }
  
    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public int docFreq() throws IOException {
      return 0;
    }
  }

}