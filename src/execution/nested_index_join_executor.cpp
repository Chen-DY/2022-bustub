//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      outer_child_executor_(std::move(child_executor)),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      tree_index_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { outer_child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID emit_rid{};
  Tuple outer_tuple{};
  std::vector<Value> values;
  while (outer_child_executor_->Next(&outer_tuple, &emit_rid)) {
    // ????
    Value value = plan_->KeyPredicate()->Evaluate(&outer_tuple, outer_child_executor_->GetOutputSchema());
    std::vector<RID> rids;
    tree_index_->ScanKey(Tuple{{value}, index_info_->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    Tuple inner_tuple{};
    if (!rids.empty()) {
      table_info_->table_->GetTuple(rids[0], &inner_tuple, exec_ctx_->GetTransaction());
      for (uint32_t i = 0; i < outer_child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(outer_tuple.GetValue(&outer_child_executor_->GetOutputSchema(), i));
      }

      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.push_back(inner_tuple.GetValue(&plan_->InnerTableSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }

    if (plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t i = 0; i < outer_child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(outer_tuple.GetValue(&outer_child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
