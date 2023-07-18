//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_index_iterator_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
                               exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())
                               ->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  // table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->name_);
  tree_index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  tree_index_iterator_ = tree_index_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tree_index_iterator_ == tree_index_->GetEndIterator()) {
    return false;
  }
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  *rid = (*tree_index_iterator_).second;
  table_info->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++tree_index_iterator_;
  return true;
}

}  // namespace bustub
