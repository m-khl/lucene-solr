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

import java.io.IOException;
import java.util.Properties;

public class EnwikiEmptyEdgeContentSource extends EnwikiContentSource {

  public static final String BODY_EDGE = org.apache.lucene.benchmark.byTask.feeds.DocMaker.BODY_FIELD+"_edge";

  @Override
  public synchronized DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
    final DocData data = super.getNextDocData(docData);
    Properties props = new Properties();
      // we need a placeholder to trigger auto reverse  
      props.put(BODY_EDGE, "f");
    data.setProps(props);
    return data;
  }
}
