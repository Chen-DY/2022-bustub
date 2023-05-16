//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>
#include <vector>

#include <cstring>
#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

// Macro for time out mechanism
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, uint64_t txn_id_start, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., txn_id_start + thread_itr, thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads, uint64_t tid,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(tid);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  Transaction *transaction = new Transaction(tid);
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    std::vector<RID> result;
    bool res = tree->GetValue(index_key, &result, transaction);
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], rid);
  }
  delete transaction;
}

const size_t NUM_ITERS = 100;
const size_t NUM_ITERS_DEBUG = 100;

void InsertTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS_DEBUG; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // keys to Insert
    std::vector<int64_t> keys;
    int64_t scale_factor = 100;
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }
    LaunchParallelTest(4, 0, InsertHelper, &tree, keys);

    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      EXPECT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      std::cout << "Current key: " << current_key << std::endl;
      std::cout << "Page ID: " << location.GetPageId() << ", Slot Num: " << location.GetSlotNum() << std::endl;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void InsertTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS_DEBUG; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // keys to Insert
    std::vector<int64_t> keys;
    int64_t scale_factor = 1000;
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }
    LaunchParallelTest(8, 0, InsertHelperSplit, &tree, keys, 2);

    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      EXPECT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // sequential insert
    std::vector<int64_t> keys = {1, 2, 3, 4, 5};
    InsertHelper(&tree, keys, 1);

    std::vector<int64_t> remove_keys = {1, 5, 3, 4};
    LaunchParallelTest(2, 1, DeleteHelper, &tree, remove_keys);

    int64_t start_key = 2;
    int64_t current_key = start_key;
    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
      size = size + 1;
    }

    EXPECT_EQ(size, 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;

    // sequential insert
    std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InsertHelper(&tree, keys, 1);

    std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
    LaunchParallelTest(2, 1, DeleteHelperSplit, &tree, remove_keys, 2);

    int64_t start_key = 7;
    int64_t current_key = start_key;
    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
      size = size + 1;
    }

    EXPECT_EQ(size, 4);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t sieve = 2;  // divide evenly
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete, 1);

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }

    EXPECT_EQ(size, for_insert.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

const int T2 = 200;
void MixTest2Call() {
  for (size_t iter = 0; iter < T2; iter++) {
    // create KeyComparator and index schema
    LOG_DEBUG("iteration %lu", iter);
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;

    // Add perserved_keys
    std::vector<int64_t> perserved_keys;
    std::vector<int64_t> dynamic_keys;
    size_t total_keys = 1000;
    size_t sieve = 5;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        perserved_keys.push_back(i);
      } else {
        dynamic_keys.push_back(i);
      }
    }
    InsertHelper(&tree, perserved_keys, 1);
    // Check there are 1000 keys in there
    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
    auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      if (((*iterator).first).ToString() % sieve == 0) {
        size++;
      }
    }

    EXPECT_EQ(size, perserved_keys.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest3Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(10, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i > 500) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete, 1);

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };

    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }

    EXPECT_EQ(size, for_insert.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest4Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i > total_keys / 2) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete, 1);
    int64_t size = 0;

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };

    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }

    EXPECT_EQ(size, for_insert.size());

    DeleteHelper(&tree, for_insert, 1);
    size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }
    EXPECT_EQ(size, 0);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

/*
 * Score: 5
 * Description: Concurrently insert a set of keys.
 */
TEST(BPlusTreeTestC2Con, InsertTest1) {
  TEST_TIMEOUT_BEGIN
  InsertTest1Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Split the concurrent insert test to multiple threads
 * without overlap.
 */
TEST(BPlusTreeTestC2Con, InsertTest2) {
  TEST_TIMEOUT_BEGIN
  InsertTest2Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Concurrently delete a set of keys.
 */
TEST(BPlusTreeTestC2Con, DeleteTest1) {
  TEST_TIMEOUT_BEGIN
  DeleteTest1Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Split the concurrent delete task to multiple threads
 * without overlap.
 */
TEST(BPlusTreeTestC2Con, DeleteTest2) {
  TEST_TIMEOUT_BEGIN
  DeleteTest2Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: First insert a set of keys.
 * Then concurrently delete those already inserted keys and
 * insert different set of keys. Check if all old keys are
 * deleted and new keys are added correctly.
 */
TEST(BPlusTreeTestC2Con, DISABLED_MixTest1) {
  TEST_TIMEOUT_BEGIN
  MixTest1Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Insert a set of keys. Concurrently insert and delete
 * a differnt set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(BPlusTreeTestC2Con, DISABLED_MixTest2) {
  TEST_TIMEOUT_BEGIN
  MixTest2Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: First insert a set of keys.
 * Then concurrently delete those already inserted keys and
 * insert different set of keys. Check if all old keys are
 * deleted and new keys are added correctly.
 */
TEST(BPlusTreeTestC2Con, DISABLED_MixTest3) {
  TEST_TIMEOUT_BEGIN
  MixTest3Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

TEST(BPlusTreeTestC2Con, DISABLED_MixTest4) {
  TEST_TIMEOUT_BEGIN
  MixTest4Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

TEST(BPlusTreeTests, InsertTest11) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid, transaction);

  auto root_page_id = tree.GetRootPageId();
  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  bpm->UnpinPage(root_page_id, false);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest12) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  int count = 0;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    // std::cout << value << "**********" << key << std::endl;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    // std::string outf = "/home/2022-bustub/build/mytree.dot";
    // tree.Draw(bpm, outf);

    std::string url = "/home/2022-bustub/build/mytree" + std::to_string(++count) + ".dot";
    tree.Draw(bpm, url);
    tree.Print(bpm);
    std::cout << "********************************************************" << std::endl;
  }
  tree.Draw(bpm, "/home/2022-bustub/build/mytree.dot");

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, SplitTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree with small order for testing split
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  // Check if the tree is split properly
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, RandomInsertTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  // BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // generate 1000 random unique keys
  std::vector<int64_t> keys{9, 101, 3, 2, 5, 7, 8, 4, 6, 10, 1, 12, 18, 20, 13, 17, 21};
  // std::vector<int64_t> keys(100);
  // std::iota(keys.begin(), keys.end(), 1);
  // std::shuffle(keys.begin(), keys.end(), std::mt19937{std::random_device{}()});
  int count = 0;
  for (auto key : keys) {
    std::cout << key << "-----------------------" << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    // String ulr = "/home/2022-bustub/build/mytree.dot";
    std::string url = "/home/2022-bustub/build/mytree" + std::to_string(count++) + ".dot";
    tree.Draw(bpm, url);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  tree.Print(bpm);
  tree.Draw(bpm, "/home/2022-bustub/build/mytree.dot");

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest13) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  // std::cout << (tree.Begin(index_key)) << std::endl;
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
