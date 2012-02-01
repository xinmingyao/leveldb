
#include "leveldb/table.h"
#include "leveldb/env.h"

#include <map>
#include <string>
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "db/filename.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include <openssl/evp.h>

using namespace leveldb;

struct KeyInfo
{
    uint16_t count;
    uint32_t bytes;

    KeyInfo(uint32_t bytes) : count(1), bytes(bytes) {}
};

typedef std::map<std::string, KeyInfo> KeyInfoDir;
typedef std::pair<std::string, KeyInfo> KeyInfoDirPair;

void print_hex(Slice s)
{
    for (int i = 0; i < s.size(); i++)
    {
        const char* format = (i < s.size()-1) ? "%d-" : "%d";
        printf(format, (unsigned char)s.data()[i]);
    }
}

void print_sha_digest(Slice s)
{
    EVP_MD_CTX mdctx;
    const EVP_MD *md = EVP_sha1();
    unsigned char md_value[EVP_MAX_MD_SIZE];
    unsigned int md_len;

    EVP_MD_CTX_init(&mdctx);
    EVP_DigestInit_ex(&mdctx, md, NULL);
    EVP_DigestUpdate(&mdctx, s.data(), s.size());
    EVP_DigestFinal_ex(&mdctx, md_value, &md_len);
    EVP_MD_CTX_cleanup(&mdctx);

    for(int i = 0; i < md_len; i++)
    {
        printf("%02x", md_value[i]);
    }
}


Status process_file(std::string filename, Env* env, KeyInfoDir& keydir)
{
    Status status;
    uint64_t size;
    status = env->GetFileSize(filename, &size);
    if (!status.ok())
    {
        printf("GetFileSize of %s failed: %s\n", filename.c_str(),
               status.ToString().c_str());
        return status;
    }

    RandomAccessFile* raf;
    status = env->NewRandomAccessFile(filename, &raf);
    if (!status.ok())
    {
        printf("new RandomAccessFile of %s failed: %s\n", filename.c_str(),
               status.ToString().c_str());
        return status;
    }

    Options options;
    Table* table;
    status = Table::Open(options, raf, size, &table);
    if (!status.ok())
    {
        printf("TableOpen of %s failed: %s\n", filename.c_str(),
               status.ToString().c_str());
        return status;
    }

    ReadOptions read_opts;
    read_opts.fill_cache = false;
    Iterator* itr = table->NewIterator(read_opts);
    KeyInfoDir::iterator keydir_itr;

    itr->SeekToFirst();
    do
    {
        print_hex(itr->key());
        printf(",");
        print_sha_digest(itr->value());
        printf(",");
        printf("%lu\n", itr->value().size());
        // Slice key = itr->key();
        // keydir_itr = keydir.find(key.ToString());
        // if (keydir_itr != keydir.end())
        // {
        //     keydir_itr->second.count += 1;
        //     keydir_itr->second.bytes += itr->value().size();
        // }
        // else
        // {
        //     KeyInfoDirPair p(key.ToString(), KeyInfo(itr->value().size()));
        //     keydir.insert(keydir_itr, p);
        // }
        itr->Next();
    } while (itr->Valid());

    delete table;
    delete raf;
    return Status::OK();
}

int main(int argc, char** argv)
{
    Env* env = Env::Default();
    KeyInfoDir keydir;
    std::vector<std::string> filenames;

    // Get a list of all files in this dir
    Status status = env->GetChildren(".", &filenames);
    if (!status.ok())
    {
        printf("GetChildren failed: %s\n", status.ToString().c_str());
        return -1;
    }

    // Open each of the table files and traverse the keys within
    int processed = 0;
    uint64_t total_bytes;
    uint64_t number;
    leveldb::FileType type;
    for(std::vector<std::string>::iterator it = filenames.begin();
        it != filenames.end(); it++)
    {
        ParseFileName(*it, &number, &type);
        if (type == kTableFile)
        {
            status = process_file(*it, env, keydir);
            if (!status.ok())
            {
                return -1;
            }

            // Update total file bytes
            uint64_t size;
            env->GetFileSize(*it, &size);
            total_bytes += size;

            processed++;
            int percent = ((processed * 1.0) / filenames.size()) * 100;
            if (processed % 100 == 0)
            {
                fprintf(stderr,"Processed %d files, %d%%\n", processed, percent);
            }
        }
    }

    // Traverse the end map and output stats
    uint16_t max_count = 0;
    for(KeyInfoDir::iterator it = keydir.begin(); it != keydir.end(); it++)
    {
        KeyInfo& ki = it->second;
        max_count = ki.count > max_count ? ki.count : max_count;
    }

    fprintf(stderr, "Total keys: %lu\n", keydir.size());
    fprintf(stderr, "Max instances of a key: %d\n", max_count);


}
