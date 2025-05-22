//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub
{

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DeleteTest1)
{
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto* transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys)
  {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys)
  {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys)
  {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys)
  {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present)
    {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key),
                remove_keys.end());
    }
    else
    {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, DeleteTest2)
{
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto* transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys)
  {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys)
  {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys)
  {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys)
  {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present)
    {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key),
                remove_keys.end());
    }
    else
    {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
TEST(BPlusTreeTests, DeleteTest3)
{
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page->GetPageId(), bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  auto* transaction = new Transaction(0);

  std::default_random_engine generator;
  std::uniform_int_distribution<int64_t> distribution(1, 10000);

  std::vector<int64_t> keys;
  for (int i = 0; i < 1000; ++i) {
    int64_t key = distribution(generator);
    keys.push_back(key);
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<int64_t> remove_keys = {keys[0], keys[keys.size() - 1]};
  for (auto key : remove_keys)
  {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys)
  {
    std::vector<RID> rids;
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (std::find(remove_keys.begin(), remove_keys.end(), key) != remove_keys.end()) {
      EXPECT_EQ(is_present, false);
    } else {
      EXPECT_EQ(is_present, true);
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size++;
    }
  }

  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
TEST(BPlusTreeTests, DeleteTest4)
{
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  auto* transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys)
  {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  index_key.SetFromInteger(10);
  tree.Remove(index_key, transaction);

  std::vector<int64_t> remove_keys = {2, 4};
  for (auto key : remove_keys)
  {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys)
  {
    std::vector<RID> rids;
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (std::find(remove_keys.begin(), remove_keys.end(), key) != remove_keys.end()) {
      EXPECT_EQ(is_present, false);
    } else {
      EXPECT_EQ(is_present, true);
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size++;
    }
  }

  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, DeleteTest5)
{
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto* bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", header_page->GetPageId(), bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  auto* transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  for (auto key : keys)
  {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<int64_t> remove_keys = {2, 4, 6};
  for (auto key : remove_keys)
  {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys)
  {
    std::vector<RID> rids;
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (std::find(remove_keys.begin(), remove_keys.end(), key) != remove_keys.end()) {
      EXPECT_EQ(is_present, false);
    } else {
      EXPECT_EQ(is_present, true);
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size++;
    }
  }

  EXPECT_EQ(size, keys.size() - remove_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}


}  // namespace bustub
