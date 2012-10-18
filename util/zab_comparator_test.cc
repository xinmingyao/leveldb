// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "zab_comparator.h"

#include "port/port.h"
#include "util/logging.h"
#include "util/testharness.h"
#include <string>
namespace zab {
  namespace comparator {

    
    class ZabComparatorTest {
    };
      
    TEST(ZabComparatorTest, Parse) {      
      char t[9];
      memset(t,0,9);
      char t2[4];
      char t3[8];
      uint8_t bucket =1;
      memcpy(t,&bucket,1);
      uint32_t epoch =1;
      uint64_t txn_id=2;

      leveldb::EncodeFixed64(t3,txn_id);
      Encode24(t+1,epoch);
      Encode40(t+4,txn_id);

      leveldb::Slice key= leveldb::Slice(t,9);
      
      char m[9];
      char m2[4];
      char m3[8];
      uint8_t b2=1;
      memset(m,0,9);
      memcpy(m,&b2,1);
      uint32_t epoch2 =1;
      uint64_t txn_id2=10;

      Encode24(m+1,epoch2);
      Encode40(m+4,txn_id2);


      leveldb::Slice key2= leveldb::Slice(m,9);
      
      
      leveldb::DB* db_;
      leveldb::Options opts;
      ZabComparatorImpl zabc =  ZabComparatorImpl(1);
      opts.comparator=&zabc;
      std::string dbname_ = leveldb::test::TmpDir() + "/db_test8";
      opts.create_if_missing = true;
      ASSERT_OK(leveldb::DB::Open(opts, dbname_, &db_));

      std::string test = std::string("test");
      std::string r2 =std::string("2222");
      
      ASSERT_EQ(-1,zabc.Compare(key,key2));
      ASSERT_EQ(1,zabc.Compare(key2,key));
      ASSERT_EQ(0,zabc.Compare(key,key));
      ASSERT_EQ(0,zabc.Compare(key2,key2));

      
      ASSERT_OK(db_->Put(leveldb::WriteOptions(), key, test));
      ASSERT_OK(db_->Put(leveldb::WriteOptions(), key2, r2));            
      
      leveldb::ReadOptions options;
      std::string result1;
      leveldb::Status s = db_->Get(options, key, &result1);
      ASSERT_EQ(test, result1);
      std::string r3;
      db_->Get(options, key2, &r3);
      ASSERT_EQ(r2, r3);
      
      char gc[9];
      memset(gc,0,9);
      // memcpy(gc,&bucket,1);
      uint64_t bb=1;
      Encode40(gc+4,bb);
      char gc_value[9];
      memset(gc_value,0,9);
      memcpy(gc_value,&bucket,1);
      Encode24(gc_value+1,epoch);
      uint64_t gc_txn =5;
      Encode40(gc_value+4,gc_txn);
      leveldb::Slice gc_slice = leveldb::Slice(gc,9);
      leveldb::Slice gc_value_slice = leveldb::Slice(gc_value,9);
      ZabKeyFactory facotry = ZabKeyFactory(1);
      uint64_t t_b =1;
      leveldb::Slice sss =facotry.getBucketGcSlice(t_b);
      ZabKey z1 =facotry.getZabKey(t);
      ZabKey z2=facotry.getZabKey(m);
      ZabKey z3 =facotry.getZabKey(gc);
      //ASSERT_EQ(gc_slice,sss);
      ASSERT_OK(db_->Put(leveldb::WriteOptions(),gc_slice, gc_value_slice));            
      ASSERT_TRUE(zabc.shouldDrop(db_,key));
      ASSERT_TRUE(!zabc.shouldDrop(db_,key2));
    }
  }
}  // namespace leveldb


int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
