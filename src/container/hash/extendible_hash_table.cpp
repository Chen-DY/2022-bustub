//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "storage/table/table_iterator.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  std::shared_ptr<Bucket> find_bucket = dir_[IndexOf(key)];
  return find_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  std::shared_ptr<Bucket> find_bucket = dir_[IndexOf(key)];
  return find_bucket->Remove(key);
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Insert the given key-value pair into the hash table.
 * If a key already exists, the value should be updated.
 * If the bucket is full and can't be inserted, do the following steps before retrying:
 *    1. If the local depth of the bucket is equal to the global depth,
 *        increment the global depth and double the size of the directory.
 *    2. Increment the local depth of the bucket.
 *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
 *
 * @param key The key to be inserted.
 * @param value The value to be inserted.
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(latch_);
  // 循环的目的是，防止桶分裂后，某个桶依旧为满的情况
  while (dir_[IndexOf(key)]->IsFull()) {
    size_t dir_index = IndexOf(key);
    std::shared_ptr<Bucket> old_bucket = dir_[dir_index];

    // 1. If the local depth of the bucket is equal to the global depth,
    //        increment the global depth and double the size of the directory.
    if (GetGlobalDepthInternal() == old_bucket->GetDepth()) {
      // global_depth_++;
      // // 将目录项翻倍
      // size_t dir_nums = dir_.size();
      // dir_.resize(dir_nums << 1);
      global_depth_++;
      size_t capacity = dir_.size() << 1;
      dir_.resize(capacity);
      // 将扩容出来的目录项暂时指向旧桶
      for (size_t i = capacity / 2; i < dir_.size(); i++) {
        dir_[i] = dir_[i - capacity / 2];
      }
    }

    // 2. Increment the local depth of the bucket.
    // old_bucket->IncrementDepth();

    // 现在的掩码
    // size_t mask = (1 << global_depth_) - 1;

    // // 3.分裂桶
    // std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index));
    // size_t new_dir_index = (0x1 << (global_depth_ - 1)) ^ dir_index;
    // // 3.1 遍历原桶中所有项，然后判断是否应该加入分裂出来的桶
    // std::list<std::pair<K, V>> item = old_bucket->GetItems();
    // typename std::list<std::pair<K,V>>::iterator iter;
    // for (iter = item.begin(); iter != item.end(); ++iter) {
    //     if ((std::hash<K>()(iter->first) & mask) == (new_dir_index & mask)){
    //       // std::cout << iter->first << std::endl;
    //       old_bucket->Remove(iter->first);
    //       // std::cout << old_bucket->Find(iter->first, iter->second) << "1111" << std::endl;
    //       new_bucket->Insert(iter->first, iter->second);
    //       // std::cout << new_bucket->Find(iter->first, iter->second) << "2222" <<std::endl;
    //     }
    // }

    int mask = 1 << GetLocalDepthInternal(dir_index);
    // 逻辑上是要把一部分元素移除旧桶,实际操作上是开两个新桶
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);
    for (auto &[k, v] : old_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(k);
      if ((hash_key & mask) == 0U) {  // ==0U是clion给我加的hhh
        zero_bucket->Insert(k, v);
      } else {
        one_bucket->Insert(k, v);
      }
    }
    num_buckets_++;

    // 重置目录指针
    // mask = 1 << (global_depth_ - 1);
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == old_bucket) {
        if ((i & mask) == 0U) {
          dir_[i] = zero_bucket;
          // dir_[i] = old_bucket;
        } else {
          // dir_[i] = new_bucket;
          dir_[i] = one_bucket;
        }
      }
    }
  }
  dir_[IndexOf(key)]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // // // 第一种
  // typename std::list<std::pair<K,V>>::iterator iter;
  // for (iter = list_.begin(); iter != list_.end(); ++iter) {
  //   if (iter->first == key) {
  //     value = iter->second;
  //     return true;
  //   }
  // }
  // return false;
  // 第二种
  return std::any_of(list_.begin(), list_.end(), [&](std::pair<K, V> p) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  typename std::list<std::pair<K, V>>::iterator iter;
  return std::any_of(list_.begin(), list_.end(), [&](std::pair<K, V> p) {
    if (p.first == key) {
      list_.remove(p);
      // std::cout << this->Find(p.first, p.second) << std::endl;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 如果桶满了，则返回
  if (IsFull()) {
    return false;
  }
  // 桶中有相同的key，更新value
  typename std::list<std::pair<K, V>>::iterator iter;
  for (iter = list_.begin(); iter != list_.end(); ++iter) {
    if (iter->first == key) {
      iter->second = value;
      return true;
    }
  }
  // 添加键值对
  list_.emplace_back(std::make_pair(key, value));
  // size_++;
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
