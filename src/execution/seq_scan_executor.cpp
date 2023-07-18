//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() {
  // if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
  //     try {
  //         bool is_locked = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
  //         LockManager::LockMode::INTENTION_SHARED, table_info_->oid_); if (!is_locked) {
  //             throw "seq_scan_executor.cpp: Lock table failed";
  //         }
  //     }
  //     catch (TransactionAbortException e) {
  //         throw "seq_scan_executor.cpp: Lock table failed" + e.GetInfo();
  //     }
  // }
  table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  do {
    if (table_iterator_ == table_info_->table_->End()) {
      //     if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      //         auto oid = table_info_->oid_;
      //         const auto rid_lock_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(oid);
      //         for (auto &rid : rid_lock_set) {
      //             exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid);
      //         }
      //         exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);

      //     }
      return false;
    }
    *tuple = *table_iterator_;
    *rid = table_iterator_->GetRid();
    ++table_iterator_;
  } while (plan_->filter_predicate_ != nullptr &&
           !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());

  // if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
  //     try {
  //         bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
  //         LockManager::LockMode::SHARED, table_info_->oid_, *rid); if (!is_locked) {
  //             throw "seq_scan_executor.cpp: Lock table failed";
  //         }
  //     }
  //     catch (TransactionAbortException e) {
  //         throw "seq_scan_executor.cpp: Lock table failed" + e.GetInfo();
  //     }
  // }
  return true;
}

}  // namespace bustub
