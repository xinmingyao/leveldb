
#include "leveldb/table.h"
#include "leveldb/env.h"

#include <map>
#include <string>
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
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

using namespace leveldb;

void print_hex(Slice& s)
{
    printf("<<");
    for (int i = 0; i < s.size(); i++)
    {
        printf("%d,", (unsigned char)s.data()[i]);
    }
    printf(">>\n");
}

int main(int argc, char** argv)
{
    char* filename = argv[1];

    Env* env = Env::Default();
    uint64_t size;
    env->GetFileSize(filename, &size);
    printf("%ld\n", size);
    RandomAccessFile* raf;
    env->NewRandomAccessFile(filename, &raf);

    Options options;
    Table* table;
    Table::Open(options, raf, size, &table);

    ReadOptions read_opts;
    Iterator* itr = table->NewIterator(read_opts);
    itr->SeekToFirst();

    // do
    // {
    //     Slice key = itr->key();
    //     print_hex(key);
    //     itr->Next();
    // } while (itr->Valid());
}
