//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 * ValueType指的是page_id_t
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndexByKey(const KeyType &key, KeyComparator comparator) const -> int {
  auto iter = std::lower_bound(array_, array_ + GetSize(), key,
                               [&comparator](const auto &pair, auto key) { return comparator(pair.first, key) < 0; });
  return std::distance(array_, iter);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndexByValue(const ValueType &value) const -> int {
  auto iter = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, iter);
}

/**
 * 在internal_page中找到最接近key的,返回page_id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, KeyComparator comparator) const -> ValueType {
  //  int l = 0;
  //  int r = this->GetSize() - 1;
  //  int mid = (l + r) / 2;

  //  while (l < r) {
  //   mid = (l + r) / 2;
  //   // 相等：0； 小于：-1；大于：1
  //   if (comparator(array_[mid].first, key) <= 0) {
  //     l = mid + 1;
  //   } else {
  //     r = mid - 1;
  //   }
  //  }
  //  return array_[l].second;
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair1, auto key) {
    return comparator(pair1.first, key) < 0;
  });
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  return std::prev(target)->second;
}

/**
 * 当split时需要调用。将页面的后一半分配个另一个页面。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *new_page, BufferPoolManager *bpm) -> void {
  int size = this->GetSize();

  int half_size = size / 2;

  for (int i = half_size; i < size; i++) {
    new_page->array_[i - half_size].first = array_[i].first;
    new_page->array_[i - half_size].second = array_[i].second;

    auto child_page = bpm->FetchPage(array_[i].second);
    auto child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_tree_page->SetParentPageId(new_page->GetPageId());
    bpm->UnpinPage(array_[i].second, true);
  }
  new_page->SetSize(size - half_size);
  this->SetSize(half_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *new_page, BufferPoolManager *bpm) {
  int cur_size = this->GetSize();
  int target_size = new_page->GetSize();

  for (int i = 0; i < cur_size; i++) {
    new_page->array_[target_size + i].first = array_[i].first;
    new_page->array_[target_size + i].second = array_[i].second;

    auto child_page = bpm->FetchPage(array_[i].second);
    auto child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_tree_page->SetParentPageId(new_page->GetPageId());
    bpm->UnpinPage(array_[i].second, true);
  }
  new_page->IncreaseSize(cur_size);
  this->SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(page_id_t insert_page_id, const KeyType &insert_key,
                                                     page_id_t old_page_id) {
  // 找到index
  // int index = 0;
  // for (auto iter : array_) {
  //   index++;
  //   if (iter.second == old_page_id) {
  //     break;
  //   }
  // }
  // index++; // 找到插入的位置
  // ValueType value;
  // auto iter = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  // int index = std::distance(array_, iter);
  // // 调整位置，并插入
  // std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  // // std::cout << index<< "&&&&&&&&&&&&&&&&&&&&&&&" << std::endl;
  // IncreaseSize(1);
  // array_[index].first = insert_key;
  // array_[index].second = insert_page_id;
  int index = FindIndexByValue(old_page_id) + 1;
  // std::cout << index << "&&&&&&&&&&&&&&&&&&&&&&&" << std::endl;
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  IncreaseSize(1);
  array_[index].first = insert_key;
  array_[index].second = insert_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToStart(const KeyType &key, const ValueType &value, BufferPoolManager *bpm) {
  std::move_backward(array_, array_ + GetSize(), array_ + 1);
  array_[0] = {key, value};
  IncreaseSize(1);
  page_id_t page_id = value;
  auto *child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(page_id)->GetData());
  child_page->SetParentPageId(GetPageId());
  bpm->UnpinPage(page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToEnd(const KeyType &key, const ValueType &value, BufferPoolManager *bpm) {
  int size = GetSize();
  array_[size] = {key, value};
  IncreaseSize(1);
  page_id_t page_id = value;
  auto *child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(page_id)->GetData());
  child_page->SetParentPageId(GetPageId());
  bpm->UnpinPage(page_id, true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
