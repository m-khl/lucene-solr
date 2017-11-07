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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestDerivedTerms extends LuceneTestCase {
  
  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    final Random r = random();
    final IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(r));
    
    iwc.setCodec(new TermsDerivingCodec(TermDerivatives.reverse, "_rev","field"));
    
    RandomIndexWriter iw = new RandomIndexWriter(r, dir, iwc);
    Document doc = new Document();
    Field field = newField("field", "", StringField.TYPE_NOT_STORED);
    Field field_rev = newField("field_rev", "", StringField.TYPE_NOT_STORED);
    doc.add(field);
    doc.add(field_rev);
    
    field_rev.setStringValue("");
    addDoc(iw, doc, field, "ABC");
    addDoc(iw, doc, field, "abc");
    addDoc(iw, doc, field, "abcdef");
    addDoc(iw, doc, field, "ijk");
    addDoc(iw, doc, field, "ebc");
    
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher is = newSearcher(ir);
    
    assert_rev(is, 1, "cba");
    assert_rev(is, 0, "CBA");
    assert_rev(is, 2, "fedcba");
    assert_rev(is, 3, "kji");
    assert_rev(is, "abc");
    
    TopDocs td = is.search(new PrefixQuery(new Term("field_rev", new BytesRef("cb"))), 5);
    assertEquals(2, td.totalHits);
    assertEquals(1, td.scoreDocs[0].doc);
    assertEquals(4, td.scoreDocs[1].doc);
    
    ir.close();
    dir.close();
  }

  protected void assert_rev(IndexSearcher is, final int i, final String text) throws IOException {
    TopDocs td = is.search(new TermQuery( new Term("field_rev", new BytesRef(text))), 5);
    assertEquals(i, td.scoreDocs[0].doc);
    assertEquals(1, td.totalHits);
  }
  
  protected void assert_rev(IndexSearcher is, final String text) throws IOException {
    TopDocs td = is.search(new TermQuery( new Term("field_rev", new BytesRef(text))), 5);
    assertEquals(0, td.totalHits);
  }

  protected void addDoc(RandomIndexWriter iw, Document doc, Field field, final String value) throws IOException {
    field.setStringValue(value);
    iw.addDocument(doc);
  }
}
