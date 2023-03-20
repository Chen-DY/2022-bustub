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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 * ValueType指的是page_id_t
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second;
}

/**
 * 在internal_page中找到最接近key的,返回page_id
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, KeyComparator comparator) const -> ValueType {
   int l = 0;
   int r = this->GetSize() - 1;
   int mid = (l + r) / 2;

   while (l < r) {
    // 相等：0； 小于：-1；大于：1
    if (comparator(array_[mid].first, key) <= 0) {
      l = mid;
    } else {
      r = mid;
    }
   }
   return array_[l].second;
}

/**
 * 当split时需要调用。将页面的后一半分配个另一个页面。
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MovaHalfTo(BPlusTreeInternalPage *new_page, BufferPoolManager *bpm) ->void {
    int size = this->GetSize();

    int half_size = size / 2;

    for (int i = half_size; i < size; i++) {
      new_page->array_[i - half_size].first = array_[i].first;
      new_page->array_[i - half_size].second = array_[i].second;
      
      auto child_page = bpm->FetchPage(array_[i].second);
      auto child_tree_page = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
      child_tree_page->SetParentPageId(new_page->GetPageId());
      bpm->UnpinPage(array_[i].second, true);
    }
    new_page->SetSize(size - half_size);
    this->SetSize(half_size);
}



// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
