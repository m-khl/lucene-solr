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
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/** this is not a map but entryset iterable and put */
public class FieldData extends AbstractMap<BytesRef,BlockTermState>{

  /* TODO steal it from injecting termsWriter */
  FixedBitSet docsSeen;
  
  public FieldData(FixedBitSet docsSeen) {
    super();
    this.docsSeen = docsSeen;
  }
  
  final BytesRefArray terms = new BytesRefArray(Counter.newCounter());
   BlockTermState [] blockStates = new BlockTermState[0];
  
  public BlockTermState put(BytesRef key, BlockTermState value) {
    final int pos = terms.append(key);
    if (blockStates.length<=pos) {
      BlockTermState[] tmpTermState = new BlockTermState[ArrayUtil.oversize(pos+1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(blockStates, 0, tmpTermState, 0, blockStates.length);
      blockStates = tmpTermState;
    }
    blockStates[pos] = value;
    //System.out.println("["+pos+"] "+key.utf8ToString()+"="+value);
    return null;
  }

  @Override
  public Set<Entry<BytesRef,BlockTermState>> entrySet() {
    return new AbstractSet<Map.Entry<BytesRef,BlockTermState>>() {

      @Override
      public Iterator<Entry<BytesRef,BlockTermState>> iterator() {
        final BytesRefIterator iter = terms.iterator();

        return new Iterator<Map.Entry<BytesRef,BlockTermState>>() {
          int pos = 0;
          SimpleEntry<BytesRef,BlockTermState> entryScratch;

          @Override
          public boolean hasNext() {
            return pos<terms.size();
          }

          @Override
          public Entry<BytesRef,BlockTermState> next() {
            final BytesRef next;
            try {
              next = iter.next();
              if (next==null) {
                assert pos == terms.size();
                throw new NoSuchElementException("yielded "+pos+" elems already");
              } 
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            if (entryScratch==null) {
              entryScratch = new SimpleEntry<BytesRef,BlockTermState>(next, null);
            } else {
              assert entryScratch.getKey()==next;
            }
            entryScratch.setValue(blockStates[pos++]);
            //System.out.println(entryScratch);
            return entryScratch;
          }
          
        };
      }

      @Override
      public int size() {
        return terms.size();
      }
    };
  }
  
}
