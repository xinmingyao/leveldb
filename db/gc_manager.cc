// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/gc_manager.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "stdio.h"
#include "malloc.h"
namespace leveldb {
  namespace gc {
    GcManager GcFactory::_gcManager;
    GcManager::GcManager(){
      
    }
    
    GcManager::~GcManager() {
    }

    void GcManager::addKeyRange(std::string prefix,std::string start,std::string end){
      KeyRange KR(start,end);
      keyRanges[prefix]=KR;
      return ;
    }
    void GcManager::deleteKeyRange(std::string prefix){
      
      keyRanges.erase(prefix);
      return ;
    }
    bool GcManager::shouldDrop(const char * key,int length){
      bool r =false;
      std::map<std::string,KeyRange>::iterator iter;
      for(iter=keyRanges.begin();iter!=keyRanges.end();iter++){
	KeyRange KR=iter->second;
	if(length!=KR._start.size()){
	  return false;
	}
	const char * prefix = iter->first.c_str();
	//if have same prefix
	if(strncmp(prefix,key,iter->first.size())!=0){
	  return false;
	}
	Slice k1(key,length);
	Slice s1(KR._start.c_str(),KR._start.size());
	Slice s2(KR._end.c_str(),KR._end.size());
	if(k1.compare(s1)>=0 && k1.compare(s2)<=0){
	    r=true;      

	    return r;
	}
      }
      return r;
    }

  }  // namespace gc
}  // namespace leveldb
