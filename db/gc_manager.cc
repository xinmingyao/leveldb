// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/gc_manager.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "stdio.h"
namespace leveldb {
  namespace gc {
    
    GcManager::GcManager(){
      
    }
    
    GcManager::~GcManager() {
    }

    Status GcManager::addKeyRange(char* prefix,char * start,char * end){
      Status s;
      KeyRange KR(start,end);
      keyRanges[prefix]=KR;
      return s;
    }
    Status GcManager::deleteKeyRange(char* prefix){
      Status s;
      keyRanges.erase(prefix);
      return s;
    }
    bool GcManager::shouldDrop(const char * key){
      bool r =false;
      std::map<char *,KeyRange,cmp_str>::iterator iter;
      for(iter=keyRanges.begin();iter!=keyRanges.end();iter++){
	KeyRange KR=iter->second;
	if(strlen(key)!=strlen(KR._start)){
	  return false;
	}
	char * prefix = iter->first;
	//if have same prefix
	if(strncmp(prefix,key,strlen(prefix))!=0){
	  return false;
	}
	Slice k1(key,strlen(key));
	Slice s1(KR._start,strlen(KR._start));
	Slice s2(KR._end,strlen(KR._end));
	if(k1.compare(s1)>=0 && k1.compare(s2)<=0){
	    r=true;
	    return r;
	}
      }
    }
    
  }  // namespace gc
}  // namespace leveldb
