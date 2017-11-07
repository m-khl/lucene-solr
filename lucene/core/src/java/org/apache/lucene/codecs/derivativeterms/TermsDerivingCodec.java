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
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene70.Lucene70Codec;
import org.apache.lucene.util.BytesRef;

public class TermsDerivingCodec extends Lucene70Codec {

  final private String derivativeFieldSuffix;
  
  final private TermsDerivingPostingsFormat hijackingPostingsFormat ;

  private final HashSet<String> fieldsToCapture;
  
  public TermsDerivingCodec(Supplier<TermMapper> termMapper, String derivativeFieldSuffix, String ... fieldsToCapt) {
   // super("TermsDerivingCodec");
    this.derivativeFieldSuffix = derivativeFieldSuffix;
    fieldsToCapture = new HashSet<String>(Arrays.asList(fieldsToCapt));
    this.hijackingPostingsFormat = new TermsDerivingPostingsFormat( 
        fieldsToCapture, derivativeFieldSuffix, termMapper);
  }
  
  @Override
  public PostingsFormat getPostingsFormatForField(String field) {

    if (fieldsToCapture.contains(field)) {
      return hijackingPostingsFormat;
    } else {
      if(field.endsWith(derivativeFieldSuffix)) {

        final String originalField = field.substring(0, field.length()-derivativeFieldSuffix.length());
        if (fieldsToCapture.contains(originalField)) {
          return hijackingPostingsFormat;
        } else {
          throw new IllegalStateException("forgot to capture "+ originalField + 
              " to inject into "+field);
        }
      }

      return super.getPostingsFormatForField(field);
    }
  }

  @FunctionalInterface
  //TODO change to Function<BytesRef, Stream<BytesRef>> ? oh my 
  interface TermMapper {
    void map(BytesRef in, Consumer<BytesRef> out) throws IOException ;
  }
}