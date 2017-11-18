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

import static org.apache.solr.common.params.CommonParams.SORT;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

/**
 * Connects to Zookeeper to pick replicas from a specific collection to send the query to.
 * Under the covers the SolrStream instances send the query to the replicas.
 * SolrStreams are opened using a thread pool, but a single thread is used
 * to iterate and merge Tuples from each SolrStream.
 * @since 5.1.0
 **/

public class CloudSolrStream extends AbstractCloudStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected StreamComparator comp;
  protected Map<String, String> fieldMappings;
  // Used by parallel stream
  protected CloudSolrStream(){
    
  }

  /**
   * @param zkHost         Zookeeper ensemble connection string
   * @param collectionName Name of the collection to operate on
   * @param params         Map&lt;String, String[]&gt; of parameter/value pairs
   * @throws IOException Something went wrong
   */
  public CloudSolrStream(String zkHost, String collectionName, SolrParams params) throws IOException {
    init(collectionName, zkHost, params);
  }

  public CloudSolrStream(StreamExpression expression, StreamFactory factory) throws IOException{   
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter aliasExpression = factory.getNamedOperand(expression, "aliases");
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != 1 + namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found",expression));
    }
    
    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }
    
    ModifiableSolrParams mParams = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("aliases")){
        mParams.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    // Aliases, optional, if provided then need to split
    if(null != aliasExpression && aliasExpression.getParameter() instanceof StreamExpressionValue){
      fieldMappings = new HashMap<>();
      for(String mapping : ((StreamExpressionValue)aliasExpression.getParameter()).getValue().split(",")){
        String[] parts = mapping.trim().split("=");
        if(2 == parts.length){
          fieldMappings.put(parts[0], parts[1]);
        }
        else{
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - alias expected of the format origName=newName",expression));
        }
      }
    }

    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    }
    else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }
    /*
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
    */
    
    // We've got all the required items
    init(collectionName, zkHost, mParams);
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp", [aliases="field=alias,..."])
    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));
    
    // collection
    expression.addParameter(collection);
    
    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        // SOLR-8409: Escaping the " is a special case.
        // Do note that in any other BASE streams with parameters where a " might come into play
        // that this same replacement needs to take place.
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(),
            val.replace("\"", "\\\"")));
      }
    }
    
    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    
    // aliases
    if(null != fieldMappings && 0 != fieldMappings.size()){
      StringBuilder sb = new StringBuilder();
      for(Entry<String,String> mapping : fieldMappings.entrySet()){
        if(sb.length() > 0){ sb.append(","); }
        sb.append(mapping.getKey());
        sb.append("=");
        sb.append(mapping.getValue());
      }
      
      expression.addParameter(new StreamExpressionNamedParameter("aliases", sb.toString()));
    }
        
    return expression;   
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());
    
    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);
    
    if(null != params){
      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      child.setExpression(mParams.getMap().entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
    }
    explanation.addChild(child);
    
    return explanation;
  }

  protected void init(String collectionName, String zkHost, SolrParams params) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = new ModifiableSolrParams(params);

    // If the comparator is null then it was not explicitly set so we will create one using the sort parameter
    // of the query. While doing this we will also take into account any aliases such that if we are sorting on
    // fieldA but fieldA is aliased to alias.fieldA then the comparater will be against alias.fieldA.

    if (params.get("q") == null) {
      throw new IOException("q param expected for search function");
    }

    if (params.getParams("fl") == null) {
      throw new IOException("fl param expected for search function");
    }
    String fls = String.join(",", params.getParams("fl"));

    if (params.getParams(SORT) == null) {
      throw new IOException("sort param expected for search function");
    }
    String sorts = String.join(",", params.getParams(SORT));
    this.comp = parseComp(sorts, fls);
  }
  
  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  private StreamComparator parseComp(String sort, String fl) throws IOException {

    String[] fls = fl.split(",");
    HashSet fieldSet = new HashSet();
    for(String f : fls) {
      fieldSet.add(f.trim()); //Handle spaces in the field list.
    }

    String[] sorts = sort.split(",");
    StreamComparator[] comps = new StreamComparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];

      String[] spec = s.trim().split("\\s+"); //This should take into account spaces in the sort spec.
      
      if (spec.length != 2) {
        throw new IOException("Invalid sort spec:" + s);
      }

      String fieldName = spec[0].trim();
      String order = spec[1].trim();
      
      if(!fieldSet.contains(spec[0])) {
        throw new IOException("Fields in the sort spec must be included in the field list:"+spec[0]);
      }
      
      // if there's an alias for the field then use the alias
      if(null != fieldMappings && fieldMappings.containsKey(fieldName)){
        fieldName = fieldMappings.get(fieldName);
      }
      
      comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
    }

    if(comps.length > 1) {
      return new MultipleFieldComparator(comps);
    } else {
      return comps[0];
    }
  }

  public static Collection<Slice> getSlices(String collectionName, ZkStateReader zkStateReader, boolean checkAlias) throws IOException {
    ClusterState clusterState = zkStateReader.getClusterState();

    Map<String, DocCollection> collectionsMap = clusterState.getCollectionsMap();

    //TODO we should probably split collection by comma to query more than one
    //  which is something already supported in other parts of Solr

    // check for alias or collection
    List<String> collections = checkAlias
        ? zkStateReader.getAliases().resolveAliases(collectionName)  // if not an alias, returns collectionName
        : Collections.singletonList(collectionName);
    // Lookup all actives slices for these collections
    List<Slice> slices = collections.stream()
        .map(collectionsMap::get)
        .filter(Objects::nonNull)
        .flatMap(docCol -> docCol.getActiveSlices().stream())
        .collect(Collectors.toList());
    if (!slices.isEmpty()) {
      return slices;
    }

    // Check collection case insensitive
    for(String collectionMapKey : collectionsMap.keySet()) {
      if(collectionMapKey.equalsIgnoreCase(collectionName)) {
        return collectionsMap.get(collectionMapKey).getActiveSlices();
      }
    }

    throw new IOException("Slices not found for " + collectionName);
  }

  @Override
  protected TupleStream createShardStream(String shardUrl, ModifiableSolrParams mParams) {
    SolrStream solrStream = new SolrStream(shardUrl, mParams);
    solrStream.setFieldMappings(this.fieldMappings);
    return solrStream;
  }
  
  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return comp;
  }

}
