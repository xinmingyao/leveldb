#ifndef ZAB_COMPARATOR_H
#define ZAB_COMPARATOR_H
#include <algorithm>
#include <stdint.h>
#include <memory.h>
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/comparator.h"
#define EPOCH_LENGTH 3
#define TXN_LENGH 5
#include "util/coding.h"
#include <malloc.h>
#include "port/port.h"
namespace zab
{
  //data from erlang use little endian
  namespace comparator
  {

    inline uint8_t Decode8(const char* ptr){
      uint8_t result;
      memcpy(&result,ptr,sizeof(result));
      return result;
    }
    
    inline uint64_t Decode40(const char* ptr){
      uint64_t lo =leveldb::DecodeFixed32(ptr);
      uint64_t hi =Decode8(ptr+4);
      return (hi<<32|lo);
	
    }
    
    inline void Encode40(char * ptr,uint64_t value){
	ptr[0]=value & 0xff;
	ptr[1]=(value>>8) & 0xff;
	ptr[2]=(value>>16) & 0xff;
	ptr[3]=(value>>24) & 0xff;
	ptr[4]=(value>>32) & 0xff;
    }
    inline void Encode24(char * ptr,uint32_t value){
     	ptr[0]=value & 0xff;
	ptr[1]=(value>>8) & 0xff;
	ptr[2]=(value>>16) & 0xff;
     
    }
    inline uint32_t Decode24(const char* ptr){
      char t[4];
      memset(t,0,4);
      memcpy(t,ptr,3);
      return leveldb::DecodeFixed32(t);
    }
  

 

    class  ZabKey{
    public:
      inline int compare(const ZabKey&b) const;
      bool shouldDrop(const ZabKey&b) const;
      uint64_t bucket;
    protected:      
      uint32_t epoch;
      uint64_t txn_id;
      
    };
    class Bucket8ZabKey :public ZabKey
    {
    public:
      Bucket8ZabKey(const char * data);
    };
    class Bucket64ZabKey :public ZabKey
    {
    public:
      Bucket64ZabKey(const char * data);
    };

    
    class ZabKeyFactory{
    public:
      ZabKeyFactory(int type_);
      ZabKey getZabKey(const char * data);
      leveldb::Slice getBucketGcSlice(uint64_t bucket);
    private:
      int type_;
      int key_length;
    };
    
    class ZabComparatorImpl : public leveldb::Comparator
    {
    public:

      ZabComparatorImpl(int bucket_type);
      ~ZabComparatorImpl();
      virtual int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const;
      virtual const char* Name() const;
      virtual void FindShortestSeparator(
          std::string* start,
          const leveldb::Slice& limit) const;
      virtual void FindShortSuccessor(std::string* key) const;
      virtual bool shouldDrop(leveldb::DB *db,const leveldb::Slice & b) const;
    private:
      ZabKeyFactory * factory_;
      };

    }

}
#endif






