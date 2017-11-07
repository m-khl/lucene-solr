#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.
#
# based on wiki 
#
# modified to use wikipedia sources and index entire docs
# currently just used to measure ingest rate

#merge.factor=mrg:10:100:10:100
#max.buffered=buf:10:10:100:100
ram.flush.mb=flush:1000:50

max.field.length=2147483647

compound=false

analyzer=org.apache.lucene.benchmark.utils.NoUrlsButEdgesAnalyzer
directory=FSDirectory
default.codec=cdc                   :org.apache.lucene.codecs.lucene70.Lucene70Codec:org.apache.lucene.benchmark.utils.DeriveBodyRevCodec
content.source=src                     :org.apache.lucene.benchmark.byTask.feeds.EnwikiEdgeContentSource:org.apache.lucene.benchmark.byTask.feeds.EnwikiEmptyEdgeContentSource
work.dir=work dir :edge:deriv

doc.stored=false
doc.tokenized=true
doc.term.vector=false
log.step=5000

docs.file=temp/enwiki-20070527-pages-articles.xml


#to inject _rev field placeholder 
doc.index.props=true

query.maker=org.apache.lucene.benchmark.byTask.feeds.InfixesQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=false
print.hits.field=docid
# -------------------------------------------------------------------------------------

{ "Rounds"

    ResetSystemErase

    { "Populate"
        CreateIndex
        { "index"
            { "MAddDocs" AddDoc > : 5000000
            CloseIndex
        }
    }
    
    OpenReader  
   
#    [ 
    {"search" Search > : 50 
   
    CloseReader 

    NewRound

    RepSumByPrefRound index
    RepSumByPrefRound search

} : 2

#RepSumByName
RepSumByPrefRound index
RepSumByPrefRound search
