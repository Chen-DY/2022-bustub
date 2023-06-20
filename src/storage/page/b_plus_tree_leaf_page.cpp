//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { this->next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  // KeyType key{};
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindIndexByKey(const KeyType &key, KeyComparator comparator) const -> int {
  auto iter = std::lower_bound(array_, array_ + GetSize(), key,
                               [&comparator](const auto &pair, auto key) { return comparator(pair.first, key) < 0; });
  return std::distance(array_, iter);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const -> const MappingType & { return array_[index]; }

/**
 * 在叶结点中找到key对应的value。
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, ValueType &value, KeyComparator comparator) -> bool {
  // int l = 0;
  // int r = this->GetSize() - 1;
  // int mid = (l + r) / 2;

  // while (l <= r) {
  //   mid = (l + r) / 2;
  //   if (comparator(array_[mid].first, key) == 0) {
  //     value = array_[mid].second;
  //     std::cout << "->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << value << std::endl;
  //     return true;
  //   }
  //   if (comparator(array_[mid].first, key) < 0) {
  //     l = mid + 1;
  //   }
  //   if (comparator(array_[mid].first, key) > 0) {
  //     r = mid - 1;
  //   }
  // }
  // std::cout << "->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << value << std::endl;
  // return false;
  auto iter = std::lower_bound(array_, array_ + GetSize(), key,
                               [&comparator](const auto &pair1, auto key) { return comparator(pair1.first, key) < 0; });
  auto index = std::distance(array_, iter);
  if (index == GetSize() || comparator(KeyAt(index), key) != 0) {
    return false;
  }
  value = array_[index].second;
  // std::cout << "->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << value << std::endl;
  return true;
}

/**
 * 在split中调用。叶结点满了，分出一半到一个新结点
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *new_page) -> void {
  int now_size = GetSize();
  int half_size = now_size / 2;

  for (int i = half_size; i < now_size; i++) {
    new_page->array_[i - half_size].first = array_[i].first;
    new_page->array_[i - half_size].second = array_[i].second;
  }

  new_page->SetNextPageId(GetNextPageId());
  SetNextPageId(new_page->GetPageId());

  new_page->SetSize(now_size - half_size);
  SetSize(half_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *new_page) -> void {
  int size = GetSize();
  int new_page_size = new_page->GetSize();
  for (int i = 0; i < size; i++) {
    new_page->array_[new_page_size + i].first = array_[i].first;
    new_page->array_[new_page_size + i].second = array_[i].second;
  }
  new_page->IncreaseSize(size);
  new_page->SetNextPageId(GetNextPageId());
  // SetNextPageId(new_page->GetNextPageId());

  this->SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator) -> void {
  // // 在叶结点中找到插入的位置
  // int locate = 0;
  // int l = 0;
  // int r = GetSize() - 1;
  // int mid = (l + r) / 2;
  // while (l <= r) {
  //   mid = (l + r) / 2;
  //   if (comparator(array_[mid].first, key) < 0) {
  //     l = mid + 1;
  //   }
  //   if (comparator(array_[mid].first, key) > 0) {
  //     r = mid - 1;
  //   }
  // }
  // // 记录位置，key大于locate的全部后移一位
  // locate = l;
  int index = FindIndexByKey(key, comparator);
  for (int i = GetSize() - 1; i >= index; --i) {
    array_[i + 1].first = array_[i].first;
    array_[i + 1].second = array_[i].second;
  }
  // 插入数据
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator comparator) -> bool {
  // // 找到删除位置
  // auto iter = std::lower_bound(array_, array_ + GetSize(), key, [&comparator](const auto &pair, auto key) {
  //   return comparator(pair.first, key) < 0;
  // });
  // int index = std::distance(array_, iter);
  int index = FindIndexByKey(key, comparator);

  if (index < 0 || index >= GetSize()) {
    return false;
  }

  if (comparator(array_[index].first, key) == 0) {
    // std::cout << "before:";
    // for (int i = 0; i < GetSize(); i++) {
    //   std::cout << array_[i].first << " ";
    // }
    // std::cout << std::endl;

    // std::cout << array_[index].first << " " << key << " " << index << std::endl;
    // 三个参数，起始迭代器，结束迭代器，目标数组的起始迭代器
    std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
    IncreaseSize(-1);

    // std::cout << "after:";
    // for (int i = 0; i < GetSize(); i++) {
    //   std::cout << array_[i].first << " ";
    // }
    // std::cout << std::endl;

    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLeftOneStep() {
  int size = GetSize();
  for (int i = 0; i < size - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  IncreaseSize(-1);
  // bpm->UnpinPage(page_id, true);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
