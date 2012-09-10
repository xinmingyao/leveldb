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
    class KeyRange {
    public:
      KeyRange(){}
      KeyRange(std::string start,std::string end1){
	_start=start;
	_end=end1;
      }
      std::string _start;
      std::string _end;
    };
    
    
    
      
      
    class GcManager {
    public:
      GcManager();
      ~GcManager();
      void addKeyRange(std::string prefix,std::string start,std::string end);
      void  deleteKeyRange(std::string prefix);
      bool shouldDrop(const char * key,int length);
      bool shouldGc();
    private:
      std::map<std::string,KeyRange> keyRanges;
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
