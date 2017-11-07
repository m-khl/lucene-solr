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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.derivativeterms.TermsDerivingCodec.TermMapper;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

final public class TermsDerivingPostingsFormat extends PostingsFormat {
  
  private final PostingsFormat postingsFormat= new Lucene50PostingsFormat(); 

  static final String NAME = "Lucene50Hijack";
  final private String derivativeFieldSuffix;
  final private Supplier<TermMapper> termMapperFactory;
  final private Set<String> fieldsToCapture;
  
  public TermsDerivingPostingsFormat() {
    this(null, null, null);
  }
  
  protected TermsDerivingPostingsFormat( 
      Set<String> fieldsToCapture, String derivativeFieldSuffix, Supplier<TermMapper> termMapperFactory) {
    super(NAME);
    this.derivativeFieldSuffix = derivativeFieldSuffix;
    this.termMapperFactory = termMapperFactory;
    this.fieldsToCapture = fieldsToCapture;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    
    final PostingsWriterBase postingsWriter = new Lucene50PostingsWriter(state);

    final Map<String, FieldData> termsByField = new HashMap<String,FieldData>(fieldsToCapture.size());
    
    final int maxDoc = state.segmentInfo.maxDoc();
    PostingsWriterBase hijack = new PostingWriterDelegate(postingsWriter) {
      public BlockTermState writeTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen) throws IOException {
        final BlockTermState state = super.writeTerm(term, termsEnum, docsSeen);
        
        final String fieldName = getFieldInfo().name;
        if (fieldsToCapture.contains(fieldName)) {
          FieldData fieldData = termsByField.get(fieldName);
          
          if (fieldData==null) {
            termsByField.put(fieldName,
                fieldData = new FieldData(new FixedBitSet(maxDoc)));
          }
          fieldData.put(BytesRef.deepCopyOf(term), state);
          fieldData.docsSeen.or(docsSeen);
          //System.out.println(termsByField);
        }
        return state;
      }
      
    };
    
    return fieldsConsumer(state, hijack, termsByField);
  }

  FieldsConsumer fieldsConsumer(SegmentWriteState state, final PostingsWriterBase postingsWriter, Map<String,FieldData> termsByField)
      throws IOException {
    boolean success = false;
    try {
      FieldsConsumer ret = new ///*ByteArray**/ 
          //TreeMap
          ByteArrayDerivativeWriter(state, postingsWriter, 
                          termsByField, derivativeFieldSuffix, termMapperFactory);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }
  
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return postingsFormat.fieldsProducer(state);
  }
}