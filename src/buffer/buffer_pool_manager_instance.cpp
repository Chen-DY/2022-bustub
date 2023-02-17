//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  // 先找free_list中的可替换的页，再从replacer中找
  frame_id_t evict_frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Evict(&evict_frame_id)) {
      return nullptr;
    }
  } else {
    evict_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // 找到page之后，如果是脏页，写回磁盘
  Page *evict_page = &pages_[evict_frame_id];
  if (evict_page->IsDirty()) {
    disk_manager_->WritePage(evict_page->GetPageId(), evict_page->GetData());
    evict_page->is_dirty_ = false;
  }
  // 解除映射
  page_table_->Remove(evict_page->GetPageId());

  // reset the memory and metadata for the new page
  *page_id = AllocatePage();
  evict_page->page_id_ = *page_id;
  evict_page->is_dirty_ = false;
  evict_page->pin_count_ = 1;
  evict_page->ResetMemory();
  // "Pin" the frame
  replacer_->RecordAccess(evict_frame_id);
  replacer_->SetEvictable(evict_frame_id, false);

  page_table_->Insert(evict_page->GetPageId(), evict_frame_id);
  return evict_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);

  Page *fetch_page = nullptr;
  frame_id_t frame_id;
  // 先看buffer pool 中是否有page
  if (page_table_->Find(page_id, frame_id)) {
    // if (pages_[page_id].pin_count_ > 0) {
    //   return nullptr;
    // }
    pages_[frame_id].pin_count_ += 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  if (!page_table_->Find(page_id, frame_id)) {
    // if (free_list_.empty() && replacer_->Size() == 0) {
    //   return nullptr;
    // }
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }
    }
  }

  // 写回数据
  fetch_page = &pages_[frame_id];
  if (fetch_page->IsDirty()) {
    disk_manager_->WritePage(fetch_page->GetPageId(), fetch_page->GetData());
    pages_[frame_id].is_dirty_ = false;
  }

  page_table_->Remove(fetch_page->GetPageId());
  // disk_manager_->ReadPage(page_id, fetch_page->GetData());
  page_table_->Insert(page_id, frame_id);

  fetch_page->page_id_ = page_id;
  fetch_page->pin_count_ = 1;
  fetch_page->is_dirty_ = false;
  fetch_page->ResetMemory();

  replacer_->RecordAccess(frame_id);
  // replacer_->SetEvictable(frame_id, false);
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  return fetch_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  Page *unpin_page = &pages_[frame_id];
  unpin_page->pin_count_ -= 1;
  if (unpin_page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  // if (is_dirty) {
  //   // disk_manager_->WritePage(page_id, unpin_page->GetData());
  //   unpin_page->is_dirty_ = is_dirty;
  // }
  unpin_page->is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (page_table_->Find(page_id, frame_id)) {
    // replacer_->Remove(frame_id);
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock_guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].is_dirty_ && pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  page_table_->Find(page_id, frame_id);
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }

  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  page_table_->Remove(page_id);

  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  // reset metadata
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();
  //  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
