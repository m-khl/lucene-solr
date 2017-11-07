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
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.derivativeterms.TermsDerivingCodec.TermMapper;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

class TreeMapDerivativeWriter extends InjectingBlockTreeTermsWriter {
  private final String fieldSuffix ;
  protected final Supplier<TermMapper> mapperFactory ;
  

  public TreeMapDerivativeWriter(SegmentWriteState state, PostingsWriterBase postingsWriter,
      Map<String,FieldData> termsByField, String fieldSuffix, Supplier<TermMapper> mapperFactory) throws IOException {
    super(state, postingsWriter, termsByField);
    this.fieldSuffix = fieldSuffix;
    this.mapperFactory = mapperFactory;
  }

  @Override
  protected String originalFieldName(String field) {
    if (field.endsWith(fieldSuffix)) {
      return field.substring(0, field.length()-fieldSuffix.length());
    } else {
      return null;
    }
  }

  @Override
  protected Map<BytesRef,BlockTermState> deriveTerms(FieldData stateByTerms) throws IOException {

    final TermMapper termMapper = mapperFactory.get();
    
    Map<BytesRef,BlockTermState> output = new TreeMap<BytesRef,BlockTermState> ();
    int termNum[]=new int[] {0};
    for(Entry<BytesRef,BlockTermState> tuple : stateByTerms.entrySet()) {
      BytesRef term = tuple.getKey();
      final BlockTermState postingOffset = tuple.getValue();
      System.out.println("mapping "+termNum[0]+"th "+term+" "+term.utf8ToString());
      termMapper.map(term,
                       (t)->{
                         final BytesRefBuilder buff = new BytesRefBuilder();
                         buff.clear();
                         buff.append(t);
                        // final int codePoint = termNum[0];
                       //  final char[] chars = Character.toChars(codePoint);
//                         if (false) {
//                           buff.grow(buff.length()+10);
//                           //buff.append(b, off, len);
//                           final int copied = UnicodeUtil.UTF16toUTF8(new String(chars), 0, chars.length, buff.bytes(),buff.length());
//                          buff.setLength(buff.length() +
//                               copied
//                                   );
//                         } else {
                           buff.append(new BytesRef(""+'\u0001'));
                           buff.append(term);
                        // }
                         final BytesRef outBr = buff.toBytesRef();
                      //   System.out.println(outBr+"~"+" term num "+codePoint+" "+Arrays.toString(chars));
                         final BlockTermState old = output.put(outBr, postingOffset);
                         assert old==null : outBr+"~"+outBr.utf8ToString();
                         }
          );
      termNum[0]++;
    }
    return output;
  }
}