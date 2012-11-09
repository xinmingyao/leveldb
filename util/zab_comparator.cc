
#include "zab_comparator.h"
namespace zab {
 
  namespace comparator {

    ZabKeyFactory::ZabKeyFactory(int t_){
      type_=t_;
      if(type_==1){
	  key_length=9;
	}else{
	  key_length=16;
	}
      };
    ZabKey ZabKeyFactory::getZabKey(const char * data){
      if(type_==1){
	ZabKey key=Bucket8ZabKey(data);
	return key;
      }else{
	ZabKey key=Bucket64ZabKey(data);
	return key;
      }
    }

    //bucket gc key,bucket:0,epoch:0,txn_id:new bucket
    leveldb::Slice ZabKeyFactory::getBucketGcSlice(uint64_t new_bucket){
      char * s =(char *)malloc(key_length);
      if(s==NULL){
	exit(-1);
      }
     memset(s,0,key_length);
     //only 40 bytes for new bucket
     //char t[8];
     //leveldb::EncodeFixed64(t,new_bucket);
     Encode40(s+key_length-TXN_LENGH,new_bucket);
    
     return leveldb::Slice(s,key_length);
   }  
    /*
      the zab key: 
      bucket:8 or 64 bit
      epoch:24
      txn_id:40 
    */   
    Bucket8ZabKey::Bucket8ZabKey(const char* data){
      bucket=Decode8(data);
      epoch=Decode24(data+1);
      txn_id=Decode40(data+4);
    }
    
    Bucket64ZabKey::Bucket64ZabKey(const char* data){
      bucket=leveldb::DecodeFixed64(data);
      epoch=Decode24(data+1);
      txn_id=Decode40(data+4);
    }
     
    inline int ZabKey::compare(const ZabKey& b) const{
      if(bucket<b.bucket)
	return -1;
      if(bucket>b.bucket)
	return 1;
      if(epoch<b.epoch){
	return -1;
      }
      if(epoch>b.epoch){
	return 1;
      }
      if(txn_id<b.txn_id){
	return -1;
      }
      if(txn_id>b.txn_id){
	return 1;
      }
      return 0;
    }
     bool ZabKey::shouldDrop(const ZabKey& b) const{
      if(bucket<b.bucket)
	return false;
      if(bucket>b.bucket)
	return false;
      if(epoch<b.epoch){
	return false;
      }
      if(epoch>b.epoch){
	return true;
      }
      if(txn_id<b.txn_id){
	return false;
      }
      if(txn_id>b.txn_id){
	return true;
      }
      return false;
    }
    ZabComparatorImpl::~ZabComparatorImpl(){
      delete factory_; 
    }
    ZabComparatorImpl::ZabComparatorImpl(int bucket_type) {
      factory_ = new ZabKeyFactory(bucket_type);
    }

    const char* ZabComparatorImpl::Name() const {
      return "leveldb.ZabComparatorImpl";
    }
 
    int ZabComparatorImpl::Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
      ZabKey a1=factory_->getZabKey(a.data());
      ZabKey b1=factory_->getZabKey(b.data());
      return a1.compare(b1);
    }
   
    void ZabComparatorImpl::FindShortestSeparator(
      std::string* start,
      const leveldb::Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

    void ZabComparatorImpl::FindShortSuccessor(std::string* key) const {
      // Find first character that can be incremented
      size_t n = key->size();
      for (size_t i = 0; i < n; i++) {
	const uint8_t byte = (*key)[i];
	if (byte != static_cast<uint8_t>(0xff)) {
	  (*key)[i] = byte + 1;
	  key->resize(i+1);
	  return;
	}
      }
      // *key is a run of 0xffs.  Leave it alone.
    }
    bool ZabComparatorImpl::shouldDrop(leveldb::DB *db,const leveldb::Slice &key) const{
      ZabKey k1= factory_->getZabKey(key.data());;
      leveldb::Slice s=factory_->getBucketGcSlice(k1.bucket);
      std::string sval;
      leveldb::ReadOptions opt;
      opt.fill_cache=true;
      leveldb::Status status = db->Get(opt,s, &sval);
      if (status.ok())
        {
	  ZabKey k3=factory_->getZabKey(sval.data());
	  //free data malloc  by factory_
	  free((void *)s.data());
	  return k3.shouldDrop(k1);
	  
	}
      //free data malloc  by factory_
      free((void *)s.data());
      return false;
    }
  }
} 
 // namespace
  // namespace zab
