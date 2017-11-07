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
import java.util.function.Supplier;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.derivativeterms.TermsDerivingCodec.TermMapper;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray.BytesRefIdxIterator;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.IntBlockPool.SliceReader;

public class ByteArrayDerivativeWriter extends TreeMapDerivativeWriter {

  private static final BytesRef separator = new BytesRef(""+'\u0001');

  public ByteArrayDerivativeWriter(SegmentWriteState state, PostingsWriterBase postingsWriter,
      Map<String,FieldData> termsByField, String fieldSuffix, Supplier<TermMapper> mapperFactory) throws IOException {
    super(state, postingsWriter, termsByField, fieldSuffix, mapperFactory);
  }

  @Override
  protected Map<BytesRef,BlockTermState> deriveTerms(FieldData stateByTerms) throws IOException {
    final TermMapper termMapper = mapperFactory.get();
    
    BytesRefHash uniqNGramms = new BytesRefHash();
    final int [] startsPosByNGrammId;
    final int [] blockPosns;
    
      final BytesRefIdxIterator inputTermDict = stateByTerms.terms.iterator();
      
      IntBlockPool ngrammIdPool = new IntBlockPool();
      final IntBlockPool.SliceWriter ngrammIdColumn = new IntBlockPool.SliceWriter(ngrammIdPool);
      int ngrammIdSlice = ngrammIdColumn.startNewSlice();
      
      IntBlockPool blockPosPool = new IntBlockPool();
      final IntBlockPool.SliceWriter blockPosColumn = new IntBlockPool.SliceWriter(blockPosPool);
      int blockPosSlice = blockPosColumn.startNewSlice();
      
      BytesRef inputTerm;
      
      final int[] totalNGramms = new int[] {0};
      
      while ((inputTerm=inputTermDict.next())!=null) {
        final BytesRef in = inputTerm;
        termMapper.map(inputTerm,
                         (t)->{
                             int ngrammId = uniqNGramms.add(t);
                             if (ngrammId>=0) {// new
                             } else {
                               ngrammId=-1-ngrammId;
                             }
                             ngrammIdColumn.writeInt(ngrammId);
                             int index = inputTermDict.index();
                             blockPosColumn.writeInt(index);
                             totalNGramms[0]++; // I'm not sure how to extract counter from that fancy writer
                             
                             /*System.out.println(totalNGramms[0]+"\t"+in.utf8ToString()+"\t"+
                                 t.utf8ToString()+"\t->\t["+ngrammId+",\t"+index+"]");*/
                           }
            );
      }
      startsPosByNGrammId = startsInDenseArray(
          ngrammIdPool, ngrammIdSlice, ngrammIdColumn.getCurrentOffset(),
          uniqNGramms);
      int [] posInGroup = startsPosByNGrammId.clone();
      SliceReader blockPosReader;
      {
        blockPosns = new int [totalNGramms[0]];
        IntBlockPool.SliceReader ngrammIdReader = new IntBlockPool.SliceReader(ngrammIdPool);    
        ngrammIdReader.reset(ngrammIdSlice, ngrammIdColumn.getCurrentOffset());
        
        blockPosReader = new IntBlockPool.SliceReader(blockPosPool);
        blockPosReader.reset(blockPosSlice, blockPosColumn.getCurrentOffset());
        
        for(int i=0;!blockPosReader.endOfSlice(); i++) {
          int ngramm = ngrammIdReader.readInt();
          assert blockPosns[posInGroup[ngramm]]==0;
          blockPosns[posInGroup[ngramm]++] = blockPosReader.readInt();
          
          assert blockPosReader.endOfSlice()==ngrammIdReader.endOfSlice();
        }
        /*
        for (int i=0;i<startsPosByNGrammId.length-1;i++) {
          BytesRef ngramm = uniqNGramms.get(i, new BytesRef());
          System.out.print("\nngrammId:"+i+"\t"+ngramm.utf8ToString()+
              " ["+startsPosByNGrammId[i]+".."+startsPosByNGrammId[i+1]+")=\t");
          for(int p=startsPosByNGrammId[i];p<startsPosByNGrammId[i+1];p++) {
            BytesRefBuilder spare = new BytesRefBuilder();
            stateByTerms.terms.get(spare, blockPosns[p]);
            System.out.print(blockPosns[p]+":"+spare.get().utf8ToString()+",");
          }
        }
        System.out.println();*/
      }
    
    final int[] sorted = uniqNGramms.sort();
    final IntBlockPool sortedPosPool;
    final int[] slicePoses;
    {
      sortedPosPool = new IntBlockPool();
      final IntBlockPool.SliceWriter sortedPosColumn = new IntBlockPool.SliceWriter(sortedPosPool);
      slicePoses = new int[uniqNGramms.size()*2];
      for(int i=0; i<uniqNGramms.size(); i++) {
        slicePoses[i*2] = sortedPosColumn.startNewSlice();
        // write ngrammId, first
        int ngrammId = sorted[i];
        sortedPosColumn.writeInt(ngrammId);
        //// DON'T write group length
        int from = startsPosByNGrammId[ngrammId];
        int to = startsPosByNGrammId[ngrammId+1];
        //sortedPosColumn.writeInt(to-from);
        // write whole group
        for (int p=from; p<to;p++) {
          sortedPosColumn.writeInt(blockPosns[p]);
        }
        slicePoses[i*2+1] = sortedPosColumn.getCurrentOffset();
      }
    }
    return new AbstractMap<BytesRef,BlockTermState>() {

      @Override
      public Set<Entry<BytesRef,BlockTermState>> entrySet() {
        return new AbstractSet<Map.Entry<BytesRef,BlockTermState>>() {

          @Override
          public Iterator<Entry<BytesRef,BlockTermState>> iterator() {
            
            return new Iterator<Map.Entry<BytesRef,BlockTermState>>() {
              Entry<BytesRef,BlockTermState> scratch;
              BytesRefBuilder outputBytes = new BytesRefBuilder();
              
              int posInGroup = 0;
              int cnt=0;
              int sliceNum=0;
              boolean needReset = true;
              int groupSize = 0;
              int ngrammLength;
              
              SliceReader sortedPosReader = new IntBlockPool.SliceReader(sortedPosPool);
              
              BytesRef buff = new BytesRef();
              @Override
              public Entry<BytesRef,BlockTermState> next() {
                if (hasNext()) {
                    if (scratch==null) {
                      scratch = new SimpleEntry<BytesRef,BlockTermState>(outputBytes.get(),null);
                    }
                    
                    if(needReset) {
                      sortedPosReader.reset(slicePoses[sliceNum*2],slicePoses[sliceNum*2+1]);
                      int ngrammId = sortedPosReader.readInt();//sorted[ngrammIndex];
                      uniqNGramms.get(ngrammId, buff);
                      groupSize = 0;
                      while(!sortedPosReader.endOfSlice()) { // how to get size of the current slice? 
                        sortedPosReader.readInt();
                        groupSize++;
                      }
                      sortedPosReader.reset(slicePoses[sliceNum*2],slicePoses[sliceNum*2+1]);
                      {
                        int ngramm2 = sortedPosReader.readInt();
                        assert ngramm2==ngrammId;
                      }
                      assert scratch.getKey()!=null;
                      
                      outputBytes.clear();
                      outputBytes.append(buff);
                      outputBytes.append(separator);
                      ngrammLength = outputBytes.length();
                      posInGroup=0;
                      needReset=false;
                      sliceNum++;// next time, next slice;
                    }
                    outputBytes.setLength(ngrammLength);
                    // add padded block pos
                    for(int shift=24;shift>=0;shift-=8) {
                      if ((groupSize>>shift&0xff)>0) {
                        outputBytes.append((byte) (posInGroup>>shift&0xff));
                      }
                    }
                    scratch.setValue(stateByTerms.blockStates[sortedPosReader.readInt()]);
                    posInGroup++;
                    cnt++;
                    if(sortedPosReader.endOfSlice()) {
                      needReset=true; 
                    }
                    /*
                    BytesRef key = scratch.getKey();
                    String r;
                    try {
                      r = key.utf8ToString();
                    }catch (Throwable e) {
                      r = key.toString();
                    }
                    System.out.println(cnt+"\t"+r+"\t"+scratch.getKey()
                    +"\t->\t"+scratch.getValue());
                    */
                  return scratch;
                }else {
                  throw new NoSuchElementException("yeilded "+cnt+" of " + blockPosns.length +" already");
                }
              }

              @Override
              public boolean hasNext() {
                return cnt < blockPosns.length;//ngramms.size();
              }
            };
          }

          @Override
          public int size() {
            return blockPosns.length;
          }
          
        };
      }
      
    };
  }

  /** @returns subj plus one meaningless trailing elem, to simplify following logic*/
  private int[] startsInDenseArray(IntBlockPool ngrammIdPool, int from, int to, BytesRefHash uniqNGramms) {
    int ngrams = uniqNGramms.size();
    
    int[] ngramData = new int[ngrams+1];
    IntBlockPool.SliceReader reader = new IntBlockPool.SliceReader(ngrammIdPool);    
    reader.reset(from, to);
    while (!reader.endOfSlice()) {
      int ngramm = reader.readInt();
      ngramData[ngramm]++;
    }
    // now we have collision list sizes in array. 
    // let's convert to starts
    int pos = 0;
    for (int i = 0; i<=ngrams; i++) {
      int prePos = pos;
      pos += ngramData[i];
      ngramData[i] = prePos;
    }
    
    return ngramData;
  }
  
}
