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

import static org.apache.solr.common.params.CommonParams.DISTRIB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

abstract class AbstractCloudStream extends TupleStream {

  protected String zkHost;
  protected String collection;
  protected ModifiableSolrParams params;
  
  protected transient Map<String, Tuple> eofTuples;
  protected transient CloudSolrClient cloudSolrClient;
  protected transient List<TupleStream> solrStreams;
  protected transient TreeSet<TupleWrapper> tuples;
  protected transient StreamContext streamContext;
  protected boolean trace;

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  /**
  * Opens the CloudSolrStream
  *
  ***/
  public void open() throws IOException {
    this.tuples = new TreeSet();
    this.solrStreams = new ArrayList();
    this.eofTuples = Collections.synchronizedMap(new HashMap());
    constructStreams();
    openStreams();
  }

  public final  Map getEofTuples() {
    return this.eofTuples;
  }

  public List<TupleStream> children() {
    return solrStreams;
  }

  protected void constructStreams() throws IOException {
    try {
  
      List<String> shardUrls = getShards(this.zkHost, this.collection, this.streamContext);
  
      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      mParams = adjustParams(mParams);
      mParams.set(DISTRIB, "false"); // We are the aggregator.
  
      for(String shardUrl : shardUrls) {
        TupleStream solrStream = createShardStream(shardUrl, mParams);
        if(streamContext != null) {
          solrStream.setStreamContext(streamContext);
        }
        solrStreams.add(solrStream);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  protected abstract TupleStream createShardStream(String shardUrl, ModifiableSolrParams mParams);

  void openStreams() throws IOException {
    ExecutorService service = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("CloudSolrStream"));
    try {
      List<Future<TupleWrapper>> futures = new ArrayList();
      for (TupleStream solrStream : solrStreams) {
        StreamOpener so = new StreamOpener((SolrStream) solrStream, getStreamSort());
        Future<TupleWrapper> future = service.submit(so);
        futures.add(future);
      }
  
      try {
        for (Future<TupleWrapper> f : futures) {
          TupleWrapper w = f.get();
          if (w != null) {
            tuples.add(w);
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      service.shutdown();
    }
  }

  /**
   *  Closes the CloudSolrStream
   **/
  public void close() throws IOException {
    if(solrStreams != null) {
      for (TupleStream solrStream : solrStreams) {
        solrStream.close();
      }
    }
  }

  public Tuple read() throws IOException {
    return _read();
  }

  protected Tuple _read() throws IOException {
    TupleWrapper tw = tuples.pollFirst();
    if(tw != null) {
      Tuple t = tw.getTuple();
  
      if (trace) {
        t.put("_COLLECTION_", this.collection);
      }
  
      if(tw.next()) {
        tuples.add(tw);
      }
      return t;
    } else {
      Map m = new HashMap();
      if(trace) {
        m.put("_COLLECTION_", this.collection);
      }
  
      m.put("EOF", true);
  
      return new Tuple(m);
    }
  }

  protected ModifiableSolrParams adjustParams(ModifiableSolrParams params) {
    return params;
  }

  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  protected class TupleWrapper implements Comparable<TupleWrapper> {
    private Tuple tuple;
    private SolrStream stream;
    private StreamComparator comp;

    public TupleWrapper(SolrStream stream, StreamComparator comp) {
      this.stream = stream;
      this.comp = comp;
    }

    public int compareTo(TupleWrapper w) {
      if(this == w) {
        return 0;
      }

      int i = comp.compare(tuple, w.tuple);
      if(i == 0) {
        return 1;
      } else {
        return i;
      }
    }

    public boolean equals(Object o) {
      return this == o;
    }

    public Tuple getTuple() {
      return tuple;
    }

    public boolean next() throws IOException {
      this.tuple = stream.read();

      if(tuple.EOF) {
        eofTuples.put(stream.getBaseUrl(), tuple);
      }

      return !tuple.EOF;
    }
  }

  protected class StreamOpener implements Callable<TupleWrapper> {

    private SolrStream stream;
    private StreamComparator comp;

    public StreamOpener(SolrStream stream, StreamComparator comp) {
      this.stream = stream;
      this.comp = comp;
    }

    public TupleWrapper call() throws Exception {
      stream.open();
      TupleWrapper wrapper = new TupleWrapper(stream, comp);
      if(wrapper.next()) {
        return wrapper;
      } else {
        return null;
      }
    }
  }
}
