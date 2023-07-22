#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    child_tuples_.push_back(child_tuple);
  }

  std::sort(
      child_tuples_.begin(), child_tuples_.end(),
      [order_bys = plan_->order_bys_, schema = child_executor_->GetOutputSchema()](const Tuple &tuple_a,
                                                                                   const Tuple &tuple_b) {
        for (const auto &order_key : order_bys) {
          switch (order_key.first) {
            case OrderByType::ASC:
            case OrderByType::DEFAULT:
            case OrderByType::INVALID:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              // return static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
              //                              .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)));
              break;
            case OrderByType::DESC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              // return static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
              //                           .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)));
              break;
          }
        }
        return false;
      });
  child_iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_iter_ == child_tuples_.end()) {
    return false;
  }

  *tuple = *child_iter_;
  *rid = tuple->GetRid();
  ++child_iter_;
  return true;
}

}  // namespace bustub
