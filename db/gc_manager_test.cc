// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/gc_manager.h"

#include "port/port.h"
#include "util/logging.h"
#include "util/testharness.h"

namespace leveldb {
  namespace gc {
    class GcManagerTest { };
    
    TEST(GcManagerTest, Parse) {
      GcManager gc;
      gc.addKeyRange("aaa","aaa100","aaa200");
      const char * k1 ="aaa100";
      ASSERT_TRUE(gc.shouldDrop("aaa100"));
      ASSERT_TRUE(gc.shouldDrop("aaa200"));
      ASSERT_TRUE(gc.shouldDrop("aaa101"));
      ASSERT_TRUE(!gc.shouldDrop("aaa000"));
      ASSERT_TRUE(!gc.shouldDrop("aaa300"));
      gc.addKeyRange("am","am100","am900");
      ASSERT_TRUE(!gc.shouldDrop("ad300"));
      ASSERT_TRUE(!gc.shouldDrop("am8"));
    }
  }
}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
