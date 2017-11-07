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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;


@SuppressSysoutChecks(bugUrl = "foo.bar")
public class TestDerivedNGramms extends LuceneTestCase {
  
  private Field field = newField("field", "", StringField.TYPE_NOT_STORED);
  private NumericDocValuesField idField = new NumericDocValuesField("id", 0);
  private int id;
  private IndexReader ir;

  public void testBasic() throws Exception {
    
    Directory dir = newDirectory();
    final Random r = random();
    final IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(r));
    
    iwc.setCodec(new TermsDerivingCodec(TermDerivatives.edge_original, "_edge","field"));
    
    RandomIndexWriter iw = new RandomIndexWriter(r, dir, iwc);
    Document doc = new Document();
    Field field_rev = newField("field_edge", "", StringField.TYPE_NOT_STORED);
    doc.add(field);
   // doc.add(idField);
    doc.add(field_rev);
    doc.add(field_rev);

    doc.add(idField);
   
    field_rev.setStringValue("f");
    final int dupes = atLeast(4);
    String abcSuffix = rarely() ? "d" : "";
    String ebcSuffix = rarely() ? "f" : "";
    for (int i=0;i<dupes;i++) {
      addDoc(iw, doc,  "abc"+abcSuffix);
      addDoc(iw, doc,  "ijk");
      addDoc(iw, doc,  "ebc"+ebcSuffix);
      addDoc(iw, doc,  "klm");
    }
    
    final int prefixes = atLeast(2000);
    int radix = Math.min(atLeast(16),Character.MAX_RADIX);
    for (int i=0; i<prefixes;i++) {
      addDoc(iw, doc, Integer.toString(i, radix).toUpperCase()+"gh");
      if (i%2==0) {
        addDoc(iw, doc, Integer.toString(i, radix).toUpperCase()+"xy");
      }
    }
    ir = iw.getReader();
    iw.close();
    
    IndexSearcher is = newSearcher(ir);
    
    NumericDocValues ndv = MultiDocValues.getNumericValues(ir, "id");
    { // two terms matches *bc*
      TopDocs td = is.search(new PrefixQuery(new Term("field_edge", new BytesRef("bc"))), 5*dupes,
          new Sort(new SortField("id", Type.LONG))
          );
      assertEquals(2*dupes, td.totalHits);
      for (int i=0;i<dupes;i++) {
        assertEquals(0+(4*i),id(ndv,td.scoreDocs[0+(i*2)].doc ))//ir.document(
            ;
        assertEquals(2+(4*i),id(ndv,td.scoreDocs[1+(i*2)].doc )
            );
      }
    }
    
    { // there is only single match for *ebc* *bcf*
      TopDocs td = is.search(new PrefixQuery(new Term("field_edge", 
          new BytesRef(ebcSuffix.length()>0 && random().nextBoolean()? "bc"+ebcSuffix :"ebc"))), 5*dupes);
      assertEquals(1*dupes, td.totalHits);
      for (int i=0;i<dupes;i++) {
        assertEquals(2+(4*i), id(ndv,td.scoreDocs[0+i].doc));
      }
    }
    
    { // no reverse
      TopDocs td = is.search(new PrefixQuery(new Term("field_edge", 
          new BytesRef(random().nextBoolean() ? "cb" : "fc"))), 5);
      assertEquals(0, td.totalHits);
    }
    
    { // two terms matches *k*
      TopDocs td = is.search(new PrefixQuery(new Term("field_edge", new BytesRef("k"))), 5*dupes);
      assertEquals(2*dupes, td.totalHits);
      for (int i=0;i<dupes;i++) {
        assertEquals(1+(4*i), id(ndv, td.scoreDocs[0+(i*2)].doc));
        assertEquals(3+(4*i), id(ndv, td.scoreDocs[1+(i*2)].doc));
      }
    }
    { 
      TopDocs td = is.search(new PrefixQuery(new Term("field_edge", 
          new BytesRef( "gh"))), prefixes);
      assertEquals(prefixes, td.totalHits);
    }
    { 
      TopDocs td = is.search(new PrefixQuery(new Term("field_edge", 
          new BytesRef( "xy"))), prefixes);
      assertEquals(prefixes/2, td.totalHits);
    }
    ir.close();
    dir.close();
  }

  private int id(NumericDocValues ndv, int docNum) throws IOException {
    if (ndv.docID()>docNum) {
      ndv = MultiDocValues.getNumericValues(ir, "id");
    }
    if(ndv.docID()!=docNum) {
      ndv.advance(docNum);
    }
    return (int) ndv.longValue();
  }

  protected void addDoc(RandomIndexWriter iw, Document doc, final String value) throws IOException {
    field.setStringValue(value);
    idField.setLongValue(id++);
    iw.addDocument(doc);
  }
}
