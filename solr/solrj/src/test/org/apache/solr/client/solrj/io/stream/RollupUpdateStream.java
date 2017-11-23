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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.Locale;
import java.util.TreeMap;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrInputDocument;

public class RollupUpdateStream extends UpdateStream {

  private FieldEqualitor overEqualitor;

  public RollupUpdateStream(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory);
    
    StreamExpressionNamedParameter overExpression = factory.getNamedOperand(expression, "over");

    if(null == overExpression || !(overExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'over' parameter listing fields to rollup by but didn't find one",expression));
    }
    
    overEqualitor = (FieldEqualitor) factory.constructEqualitor(((StreamExpressionValue)overExpression.getParameter()).getValue(), FieldEqualitor.class);
  }
  
  @Override
  protected SolrInputDocument convertTupleToSolrDocument(Tuple tuple) {
    SolrInputDocument doc = new SolrInputDocument();
    Object bucket = tuple.get(overEqualitor.getLeftFieldName());
    doc.addField("id", bucket);
    doc.addField("metric_count(*)", new TreeMap<String,Integer>() {{put("inc",1);}});
    System.out.println(doc);
    return doc;
  }



  
}
