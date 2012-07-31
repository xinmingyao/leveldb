// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_GC_MANAGER_H_
#define STORAGE_LEVELDB_DB_GC_MANAGER_H_

#include <stdint.h>
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/env.h"
#include <map>
#include <string>
namespace leveldb {
  namespace gc{
    class GcManager;
    class GcFactory;
    struct KeyRange {
      KeyRange(){}
    KeyRange(char * start,char * end1)
    : _start(start),_end(end1){}
      char * _start;
      char * _end;
    };
    
    struct cmp_str	{
      bool operator()(char const *a,char const * b){
	Slice c1(a,strlen(a));
	Slice c2(b,strlen(b));
	return c1.compare(c2);
      }
      
    };
    class GcManager {
    public:
      GcManager();
      ~GcManager();
      Status addKeyRange(char* prefix,char * start,char * end);
      Status deleteKeyRange(char* prefix);
      bool shouldDrop(const char * key);
    private:
      std::map<char *,KeyRange,cmp_str> keyRanges;
    };
    
    class GcFactory{
      
    public:
      static GcManager  _gcManager;
      static GcManager * getGcManager(){
      return &_gcManager;
      }
    };
    
  }
}  // namespace leveldb

#endif  
