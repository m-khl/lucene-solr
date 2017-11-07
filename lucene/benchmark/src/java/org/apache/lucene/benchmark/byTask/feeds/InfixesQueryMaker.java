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
package org.apache.lucene.benchmark.byTask.feeds;

import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;

public class InfixesQueryMaker extends AbstractQueryMaker {

  private static String[] STANDARD_QUERIES = { "Images catbox gif",
      "Imunisasi haram", "Favicon ico", "Michael jackson", "Unknown artist",
      "Lily Thai", "Neda", "The Last Song", "Metallica", "Nicola Tesla",
      "Max B", "Skil Corporation", "\"The 100 Greatest Artists of All Time\"",
      "\"Top 100 Global Universities\"", "Pink floyd", "Bolton Sullivan",
      "Frank Lucas Jr", "Drake Woods", "Radiohead", "George Freeman",
      "Oksana Grigorieva", "The Elder Scrolls V", "Deadpool", "Green day",
      "\"Red hot chili peppers\"", "Jennifer Bini Taylor",
      "The Paradiso Girls", "Queen", "3Me4Ph", "Paloma Jimenez", "AUDI A4",
      "Edith Bouvier Beale: A Life In Pictures", "\"Skylar James Deleon\"",
      "Simple Explanation", "Juxtaposition", "The Woody Show", "London WITHER",
      "In A Dark Place", "George Freeman", "LuAnn de Lesseps", "Muhammad.",
      "U2", "List of countries by GDP", "Dean Martin Discography", "Web 3.0",
      "List of American actors", "The Expendables",
      "\"100 Greatest Guitarists of All Time\"", "Vince Offer.",
      "\"List of ZIP Codes in the United States\"", "Blood type diet",
      "Jennifer Gimenez", "List of hobbies", "The beatles", "Acdc",
      "Nightwish", "Iron maiden", "Murder Was the Case", "Pelvic hernia",
      "Naruto Shippuuden", "campaign", "Enthesopathy of hip region",
      "operating system", "mouse",
      "List of Xbox 360 games without region encoding", "Shakepearian sonnet",
      "\"The Monday Night Miracle\"", "India", "Dad's Army",
      "Solanum melanocerasum", "\"List of PlayStation Portable Wi-Fi games\"",
      "Little Pixie Geldof", "Planes, Trains & Automobiles", "Freddy Ingalls",
      "The Return of Chef", "Nehalem", "Turtle", "Calculus", "Superman-Prime",
      "\"The Losers\"", "pen-pal", "Audio stream input output", "lifehouse",
      "50 greatest gunners", "Polyfecalia", "freeloader", "The Filthy Youth" ,
      "night","trading", "ford","credit"  };
  
  @Override
  protected Query[] prepareQueries() throws Exception {
    List<Query> rez = new LinkedList<Query>();
    try(Analyzer analyzer = getEdgeAnalyzer()){
      for (String text:STANDARD_QUERIES) {
        final TokenStream stream = analyzer.tokenStream("body", text);
        final CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
        stream.reset();
        while(stream.incrementToken()) {
          rez.add(new PrefixQuery(new Term(EnwikiEmptyEdgeContentSource.BODY_EDGE,term.toString())));
        }
        stream.close();
      }
    }
    return rez.toArray(new Query[] {});
  }

  private Analyzer getEdgeAnalyzer() {
    return new StopwordAnalyzerBase(StandardAnalyzer.ENGLISH_STOP_WORDS_SET) {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer src = new StandardTokenizer();

        TokenStream tok = new StandardFilter(src);
        tok = new LowerCaseFilter(tok);
        tok = new StopFilter(tok, stopwords);
        tok = new EdgeNGramTokenFilter(tok, 3, 10);
        tok = new LengthFilter(tok,3, 20);
        return new TokenStreamComponents(src, tok);
      }
    };
  }

}
