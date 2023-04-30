#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *transaction) -> Page * {
  // auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  // auto *cur_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // if (operation == Operation::SEARCH) {
  //   root_page_id_latch_.RUnlock();
  //   page->RLatch();
  // } else {
  //   page->WLatch();
  //   if (operation == Operation::DELETE && cur_page->GetSize() > 2) {
  //     ReleaseLatchFromQueue(transaction);
  //   }
  //   if (operation == Operation::INSERT && cur_page->IsLeafPage() && cur_page->GetSize() < cur_page->GetMaxSize() - 1)
  //   {
  //     ReleaseLatchFromQueue(transaction);
  //   }
  //   if (operation == Operation::INSERT && !cur_page->IsLeafPage() && cur_page->GetSize() < cur_page->GetMaxSize()) {
  //     ReleaseLatchFromQueue(transaction);
  //   }
  // }

  // while (!cur_page->IsLeafPage()) {
  //   // auto *internal_page = reinterpret_cast<InternalPage *>(page->GetData());
  //   auto *internal_page = reinterpret_cast<InternalPage *>(cur_page);
  //   auto child_page_id = internal_page->LookUp(key, comparator_);
  //   buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);

  //   auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
  //   auto child_cur_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  //   if (operation == Operation::SEARCH) {
  //     child_page->RLatch();
  //     page->RUnlatch();
  //     buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  //   } else if (operation == Operation::INSERT) {
  //     child_page->WLatch();
  //     transaction->AddIntoPageSet(page);

  //     if (child_cur_page->IsLeafPage() && child_cur_page->GetSize() < child_cur_page->GetMaxSize() - 1) {
  //       ReleaseLatchFromQueue(transaction);
  //     }
  //     if (!child_cur_page->IsLeafPage() && child_cur_page->GetSize() < child_cur_page->GetMaxSize()) {
  //       ReleaseLatchFromQueue(transaction);
  //     }
  //   } else if (operation == Operation::DELETE) {
  //     child_page->WLatch();
  //     transaction->AddIntoPageSet(page);

  //     if (child_cur_page->GetSize() > child_cur_page->GetMinSize()) {
  //       ReleaseLatchFromQueue(transaction);
  //     }
  //   }
  //   page = child_page;
  //   cur_page = child_cur_page;
  // }

  // return page;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (operation == Operation::SEARCH) {
    root_page_id_latch_.RUnlock();
    page->RLatch();
  } else {
    page->WLatch();
    if (operation == Operation::DELETE && node->GetSize() > 2) {
      ReleaseLatchFromQueue(transaction);
    }
    if (operation == Operation::INSERT && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1) {
      ReleaseLatchFromQueue(transaction);
    }
    if (operation == Operation::INSERT && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize()) {
      ReleaseLatchFromQueue(transaction);
    }
  }

  while (!node->IsLeafPage()) {
    auto *i_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_node_page_id;
    child_node_page_id = i_node->LookUp(key, comparator_);

    assert(child_node_page_id > 0);

    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (operation == Operation::SEARCH) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else if (operation == Operation::INSERT) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      // child node is safe, release all locks on ancestors
      if (child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize() - 1) {
        ReleaseLatchFromQueue(transaction);
      }
      if (!child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    } else if (operation == Operation::DELETE) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      // child node is safe, release all locks on ancestors
      if (child_node->GetSize() > child_node->GetMinSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    }

    page = child_page;
    node = child_node;
  }

  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeave(const KeyType &key) const -> Page * {
  // root_page_latch_.RLock();
  // 找到根结点,然后将page强转为BPlusTreePage类型
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *root_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!root_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    auto child_page_id = internal_page->LookUp(key, comparator_);
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    root_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  // 这里没有unpinPage，需要在调用后unpin
  return page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_page_id_latch_.RLock();
  bool is_reslut = false;

  Page *page = FindLeaf(key, Operation::SEARCH, transaction);
  // Page *page = FindLeave(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  is_reslut = leaf_page->LookUp(key, value, comparator_);

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (is_reslut) {
    result->emplace_back(value);
  }
  return is_reslut;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  // 当前b+树为空
  if (IsEmpty()) {
    StartNewTree(key, value);
    ReleaseLatchFromQueue(transaction);
    // Print(buffer_pool_manager_);
    return true;
  }
  // Page *page = FindLeave(key);
  Page *page = FindLeaf(key, Operation::INSERT, transaction);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  // 如果存在相同的key，就不用插入直接返回false
  ValueType val;
  if (leaf_page->LookUp(key, val, comparator_)) {
    ReleaseLatchFromQueue(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    // Print(buffer_pool_manager_);
    return false;
  }

  // 叶子结点没满，正常插入，返回就行
  leaf_page->Insert(key, value, comparator_);
  int now_leaf_size = leaf_page->GetSize();
  if (now_leaf_size < leaf_max_size_) {
    ReleaseLatchFromQueue(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    // Print(buffer_pool_manager_);
    return true;
  }

  // 叶子结点满了，需要split
  auto *new_leaf_page = reinterpret_cast<LeafPage *>(SplitPage(leaf_page));
  InsertToParent(leaf_page, new_leaf_page, new_leaf_page->KeyAt(0), transaction);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  // Print(buffer_pool_manager_);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitPage(BPlusTreePage *page) -> BPlusTreePage * {
  page_id_t page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&page_id);

  if (page->IsLeafPage()) {
    auto *new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    auto *old_leaf_page = reinterpret_cast<LeafPage *>(page);

    new_leaf_page->Init(page_id, old_leaf_page->GetParentPageId(), leaf_max_size_);
    old_leaf_page->MoveHalfTo(new_leaf_page);
  } else {
    auto *new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    auto *old_internal_page = reinterpret_cast<InternalPage *>(page);

    new_internal_page->Init(page_id, old_internal_page->GetParentPageId(), internal_max_size_);
    old_internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  }
  return reinterpret_cast<BPlusTreePage *>(new_page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToParent(BPlusTreePage *page, BPlusTreePage *new_page, const KeyType &new_page_key,
                                    Transaction *transaction) {
  // 如果分裂的结点是根结点,创建一个根节点，然后分配page和new_page之间的信息
  if (page->IsRootPage()) {
    // auto temp_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto *root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root_page->SetKeyAt(1, new_page_key);
    root_page->SetValueAt(0, page->GetPageId());
    root_page->SetValueAt(1, new_page->GetPageId());

    page->SetParentPageId(root_page_id_);
    new_page->SetParentPageId(root_page_id_);

    root_page->SetSize(2);
    UpdateRootPageId(0);

    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);

    return;
  }

  // 如果插入ParentPage没有满，则直接插入即可；反之需要再次split(),即递归向上
  page_id_t parent_page_id = page->GetParentPageId();
  auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  if (parent_page->GetSize() < internal_max_size_) {
    parent_page->InsertNodeAfter(new_page->GetPageId(), new_page_key, page->GetPageId());
    ReleaseLatchFromQueue(transaction);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    // std::cout << (parent_page_id == page->GetPageId()) << "qqqqqqqqqqqqqq" << std::endl;
    return;
  }
  parent_page->InsertNodeAfter(new_page->GetPageId(), new_page_key, page->GetPageId());
  // page_id_t new_parent_page_id;
  // auto *new_parent_page = reinterpret_cast<InternalPage
  // *>(buffer_pool_manager_->NewPage(&new_parent_page_id)->GetData());
  auto *new_parent_page = reinterpret_cast<InternalPage *>(SplitPage(parent_page));
  InsertToParent(parent_page, new_parent_page, new_parent_page->KeyAt(0), transaction);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
}

/**
 * 插入时，B+树为空。
 * 创建根节点，并插入值
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) -> void {
  // page_id_t root_page_id;
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  auto *root_page = reinterpret_cast<LeafPage *>(page->GetData());

  root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root_page->Insert(key, value, comparator_);
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  if (IsEmpty()) {
    ReleaseLatchFromQueue(transaction);
    return;
  }

  // Page *page = FindLeave(key);
  Page *page = FindLeaf(key, Operation::DELETE, transaction);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  bool is_remove = leaf_page->Remove(key, comparator_);
  // 如果没有找到index，则直接返回
  if (!is_remove) {
    ReleaseLatchFromQueue(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }

  // 如果结点数量正确，则normal delete，直接返回结果
  if (is_remove && leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    ReleaseLatchFromQueue(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }
  // 不是正常删除，需要重新分配
  RedistributeOrMerge(leaf_page, transaction);
  page->WUnlatch();
  // transaction->AddIntoDeletedPageSet(page->GetPageId());
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(), [&bpm = buffer_pool_manager_](const page_id_t page_id) {
    bpm->DeletePage(page_id);
  });
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeOrMerge(BPlusTreePage *cur_page, Transaction *transaction) {
  if (cur_page->IsRootPage()) {
    if (cur_page->IsLeafPage() && cur_page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      ReleaseLatchFromQueue(transaction);
      transaction->AddIntoDeletedPageSet(cur_page->GetPageId());
      return;
    }
    if (!cur_page->IsLeafPage() && cur_page->GetSize() == 1) {
      auto *root_page = reinterpret_cast<InternalPage *>(cur_page);
      auto child_page = buffer_pool_manager_->FetchPage(root_page->ValueAt(0));
      auto child_leaf_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_leaf_page->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = child_leaf_page->GetPageId();
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      ReleaseLatchFromQueue(transaction);
      transaction->AddIntoDeletedPageSet(cur_page->GetPageId());
      return;
    }
    return;
  }
  // std::cout << "看下Size 2-----" << cur_page->GetSize() << "----" << cur_page->GetMinSize() << std::endl;
  if (cur_page->GetSize() >= cur_page->GetMinSize()) {
    ReleaseLatchFromQueue(transaction);
    return;
  }

  // 找到parent_page，并找到
  Page *parent = buffer_pool_manager_->FetchPage(cur_page->GetParentPageId());
  auto *parent_page = reinterpret_cast<InternalPage *>(parent->GetData());
  // int parent_page_index = parent_page->FindIndexByValue(parent_page->GetPageId());
  int cur_page_index = parent_page->FindIndexByValue(cur_page->GetPageId());

  // page_id_t cur_page_id = cur_page->GetPageId();
  page_id_t left_sibling_page_id = parent_page->ValueAt(cur_page_index - 1);
  page_id_t right_sibling_page_id = parent_page->ValueAt(cur_page_index + 1);

  // 首先判断能不能够向左右孩子借结点，然后让B+树保持平衡。若不能向左右孩子借结点，则需要merge
  if (cur_page_index > 0) {
    Page *left_page = buffer_pool_manager_->FetchPage(left_sibling_page_id);
    left_page->WLatch();
    auto *left_sibling_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());

    if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
      RedistributeLeft(left_sibling_page, cur_page, parent_page, cur_page_index);
      ReleaseLatchFromQueue(transaction);
      left_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), false);
  }

  if (cur_page_index < parent_page->GetSize() - 1) {
    Page *right_page = buffer_pool_manager_->FetchPage(right_sibling_page_id);
    right_page->WLatch();
    auto *right_sibling_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
      RedistributeRight(right_sibling_page, cur_page, parent_page, cur_page_index);
      ReleaseLatchFromQueue(transaction);
      right_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), false);
  }

  if (cur_page_index > 0) {
    Page *left_page = buffer_pool_manager_->FetchPage(left_sibling_page_id);
    left_page->WLatch();
    auto *left_sibling_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
    Merge(left_sibling_page, cur_page, parent_page, cur_page_index, transaction);
    transaction->AddIntoDeletedPageSet(cur_page->GetPageId());
    ReleaseLatchFromQueue(transaction);
    left_page->WUnlatch();

    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  if (cur_page_index < parent_page->GetSize() - 1) {
    Page *right_page = buffer_pool_manager_->FetchPage(right_sibling_page_id);
    right_page->WLatch();
    auto *right_sibling_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    Merge(cur_page, right_sibling_page, parent_page, cur_page_index + 1, transaction);

    transaction->AddIntoDeletedPageSet(cur_page->GetPageId());
    ReleaseLatchFromQueue(transaction);
    right_page->WUnlatch();

    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *left_page, BPlusTreePage *right_page, InternalPage *parent, int index, Transaction *transaction) {
  if (right_page->IsLeafPage()) {
    auto *left_leaf_page = reinterpret_cast<LeafPage *>(left_page);
    auto *right_leaf_page = reinterpret_cast<LeafPage *>(right_page);
    right_leaf_page->MoveAllTo(left_leaf_page);

  } else {
    auto *left_internal_page = reinterpret_cast<InternalPage *>(left_page);
    auto *right_internal_page = reinterpret_cast<InternalPage *>(right_page);
    right_internal_page->MoveAllTo(left_internal_page, buffer_pool_manager_);
  }
  parent->Remove(index);
  RedistributeOrMerge(parent, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeLeft(BPlusTreePage *left_sibling_page, BPlusTreePage *cur_page,
                                      InternalPage *parent_page, int index) {
  KeyType left_key;
  if (left_sibling_page->IsLeafPage()) {
    auto *left_page = reinterpret_cast<LeafPage *>(left_sibling_page);
    auto *target_page = reinterpret_cast<LeafPage *>(cur_page);
    int left_index = left_page->GetSize() - 1;
    left_key = left_page->KeyAt(left_index);
    target_page->Insert(left_key, left_page->ValueAt(left_index), comparator_);
    left_page->IncreaseSize(-1);
  } else {
    auto *left_page = reinterpret_cast<InternalPage *>(left_sibling_page);
    auto *target_page = reinterpret_cast<InternalPage *>(cur_page);
    int left_index = left_page->GetSize() - 1;
    left_key = left_page->KeyAt(left_index);
    target_page->InsertToStart(left_key, left_page->ValueAt(left_index), buffer_pool_manager_);
    left_page->IncreaseSize(-1);
  }
  parent_page->SetKeyAt(index, left_key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeRight(BPlusTreePage *right_sibling_page, BPlusTreePage *cur_page,
                                       InternalPage *parent_page, int index) {
  KeyType right_key;
  if (right_sibling_page->IsLeafPage()) {
    auto *right_page = reinterpret_cast<LeafPage *>(right_sibling_page);
    auto *target_page = reinterpret_cast<LeafPage *>(cur_page);
    right_key = right_page->KeyAt(0);
    target_page->Insert(right_key, right_page->ValueAt(0), comparator_);
    right_page->IncreaseSize(-1);
  } else {
    auto *right_page = reinterpret_cast<InternalPage *>(right_sibling_page);
    auto *target_page = reinterpret_cast<InternalPage *>(cur_page);
    right_key = right_page->KeyAt(0);
    target_page->InsertToEnd(right_key, right_page->ValueAt(0), buffer_pool_manager_);
    right_page->MoveLeftOneStep();
    // 更新一下父节点的key
    right_key = right_page->KeyAt(0);
    // right_page->IncreaseSize(-1);
  }
  parent_page->SetKeyAt(index + 1, right_key);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  // root_page_id_latch_.RLock();
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *root_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!root_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(root_page);
    page_id_t page_id = internal_page->ValueAt(0);
    root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  }
  auto *leaf_page = reinterpret_cast<LeafPage *>(root_page);

  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto *page = FindLeaf(key, Operation::SEARCH, nullptr);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf_page->FindIndexByKey(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *root_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!root_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(root_page);
    // unpin
    // buffer_pool_manager_->UnpinPage(root_page->GetPageId(), false);
    page_id_t page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  }
  auto *leaf_page = reinterpret_cast<LeafPage *>(root_page);
  // std::cout << leaf_page->GetPageId() << "-----ddddddddddddddddddddddddddd" << std::endl;
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_page->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) -> void {
  while (!transaction->GetPageSet()->empty()) {
    auto *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      this->root_page_id_latch_.WUnlock();
    } else {
      page->WUnlatch();
      // 为什么是false？
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  root_page_id_latch_.RLock();
  root_page_id_latch_.RUnlock();
  return root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page GetPageId(): " << leaf->GetPageId()
              << "  ------- parent GetParentPageId(): " << leaf->GetParentPageId()
              << "  ------- next GetNextPageId(): " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << "key:" << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page GetPageId(): " << internal->GetPageId()
              << "  ------- parent GetParentPageId(): " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << "key : value " << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",   ";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
