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
import java.io.StringReader;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.derivativeterms.TermsDerivingCodec.TermMapper;
import org.apache.lucene.util.BytesRef;

public class TermDerivatives {

  /** yields reversed terms */
  public static final Supplier<TermMapper> reverse = () -> {return new TermMapper() {
    
    final private Tokenizer in = new KeywordTokenizer();
    final private TokenFilter out = new ReverseStringFilter(in);
    final private CharTermAttribute charTerm = out.addAttribute(CharTermAttribute.class);
    
    @Override
    public void map(BytesRef input, Consumer<BytesRef> output) throws IOException {
      in.setReader(new StringReader(input.utf8ToString()));
      out.reset();
      while (out.incrementToken()) {
        BytesRef copy = new BytesRef(charTerm);
        output.accept(copy);
      }
      out.end();
      out.close();
    }
    
  };
  };
  /** yields edgengramms concatenated with original term*/
  public static final Supplier<TermMapper> edge_original = () -> {return new TermMapper() {
    
    final private KeywordTokenizer in = new KeywordTokenizer();
    final private TokenFilter out = new ReverseStringFilter(
                                          new EdgeNGramTokenFilter(
                                              new ReverseStringFilter(in), 1, 255));
    final private CharTermAttribute charTerm = out.addAttribute(CharTermAttribute.class);
    final private StringBuilder buffer = new StringBuilder();
    
    @Override
    public void map(BytesRef input, Consumer<BytesRef> output) throws IOException {
      final String inputString = input.utf8ToString();
      in.setReader(new StringReader(inputString));
      out.reset();
      while (out.incrementToken()) {
        buffer.setLength(0);
        buffer.append(charTerm);
       // buffer.append(inputString);
        BytesRef copy = new BytesRef(buffer);
        
        //System.out.println(copy.utf8ToString());
        
        output.accept(copy);
      }
      out.end();
      out.close();
    }
    
  };
  };

  private TermDerivatives() {}
  
  
}
