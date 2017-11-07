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
package org.apache.lucene.benchmark.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiEmptyEdgeContentSource;
import org.apache.lucene.util.LuceneTestCase;

public class NoUrlsAnalyzerTest extends LuceneTestCase{

  public void testNoURLs() throws IOException {
    try( Analyzer analyzer = new NoUrlsButEdgesAnalyzer()) {
      final TokenStream tokens = analyzer.tokenStream("body", "Encyclopædia Britannica. 2006. Encyclopædia Britannica Premium Service. [[29 August]] [[2006]] &lt;http://www.britannica.com/eb/article-9117285&gt;. Anarchism is &quot;a cluster of doctrines and attitudes centred ");
      CharTermAttribute term = tokens.addAttribute(CharTermAttribute.class);
      tokens.reset();
      while(tokens.incrementToken()) {
        final String token = term.toString();
        assertFalse(token, token.contains("http"));
      }  
    }
  }
  
  public void testEdges() throws IOException {
    try (Analyzer analyzer = new NoUrlsButEdgesAnalyzer()) {
      final List<String> qbf = Arrays.asList("quick", "brown", "fox");
      assertTokensOnFields(analyzer, "the quick brown fox",
          new String[] {DocMaker.BODY_FIELD},
          qbf);

      final List<String> edgeTokens = qbf.stream().flatMap((t) -> {
        return tailEdges(t);
      }).collect(Collectors.toList());
      Arrays.asList("quick", "brown", "fox");
      assertTokensOnFields(analyzer, "the quick brown fox",
          new String[] {EnwikiEmptyEdgeContentSource.BODY_EDGE},
          edgeTokens);
    }
  }

  Stream<String> tailEdges(String t) {
    return IntStream.range(1, t.length()+1)
        .<String>mapToObj((i) -> {
          return t.substring(t.length() - i, t.length());
        });
  }

  void assertTokensOnFields(Analyzer analyzer, final String text, final String[] fields,
      final List<String> tokensExpected) throws IOException {
    for (String field:fields) {
      final TokenStream tokens = analyzer.tokenStream(field, text);
      CharTermAttribute term = tokens.addAttribute(CharTermAttribute.class);
      tokens.reset();
      final Iterator<String> qbf = tokensExpected.iterator();
      while(tokens.incrementToken()) {
        final String token = term.toString();
        assertEquals(qbf.next(), token);
      }  
      tokens.close();
    }
  }
}
