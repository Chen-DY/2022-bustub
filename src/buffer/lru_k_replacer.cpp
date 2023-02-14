//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cmath>
#include <algorithm>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <memory>
#include <utility>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex> guard(latch_);
    if (!history_list_.empty()) {
        for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
            if ((*iter)->is_evictable_) {
                size_t del_frame_id = (*iter)->frame_id_;
                *frame_id = del_frame_id;
                frame_map_.erase(del_frame_id);
                history_list_.remove(*iter);
                curr_size_--;
                return true;
            }
        }
    }

    if (!cache_list_.empty()) {
        for (auto iter = cache_list_.begin(); iter != cache_list_.end(); iter++) {
            if ((*iter)->is_evictable_) {
                size_t del_frame_id = (*iter)->frame_id_;
                *frame_id = del_frame_id;
                frame_map_.erase(del_frame_id);
                cache_list_.remove(*iter);
                curr_size_--;
                return true;
            }
        }
    }
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);

    if (frame_map_.size() > replacer_size_) {
        throw "frame_id_ is invalid!";
    }

    auto iter = frame_map_.find(frame_id);
    // 如果在map中通过frame_id没有找到，则想history_list_中插入，并初始化相应的值
    if (iter == frame_map_.end()) {
        FrameInfo new_history_frame = {frame_id, 1, false};
        std::unique_ptr<FrameInfo> ff = std::make_unique<FrameInfo>(new_history_frame);
        history_list_.emplace_back(std::move(ff));
        frame_map_[frame_id] = std::prev(history_list_.end());
    } 
    // 如果map中能找到，且k_distance_ 小于 k_，则在history_list_中找到，并修改相应的值
    if (iter != frame_map_.end() && (*iter->second)->k_distance_ < k_) {
        (*iter->second)->k_distance_++;
        if ((*iter->second)->k_distance_ == k_) {  // 当距离够了，应该加入cache_list_
            cache_list_.emplace_back(std::move(*iter->second));
            history_list_.remove(*iter->second);
            frame_map_[frame_id] = (std::prev(cache_list_.end()));
        }
        if ((*iter->second)->k_distance_ < k_) { // 如果距离不够，更新在history中的位置
            auto temp = std::move(*iter->second);
            history_list_.remove(*iter->second);
            // std::cout << (*iter).first << std::endl; 
            history_list_.emplace_back(temp.release());   // release() 和 move()一个效果
            frame_map_[frame_id] = (std::prev(history_list_.end()));
        }
    }
    
    // 如果map中能找到，且k_distance_ 大于 k_，则在cache_list_中找到，只需调整位置
    if (iter != frame_map_.end() && (*iter->second)->k_distance_ >= k_) {
        auto temp = std::move(*iter->second);
        cache_list_.remove(*iter->second);
        cache_list_.emplace_back(temp.release());
        frame_map_[frame_id] = (std::prev(cache_list_.end()));
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> guard(latch_);
    auto iter = frame_map_.find(frame_id);
    // 非法frame_id，直接返回
    if (iter == frame_map_.end()) {
        return;
    }
    
    if (!(*frame_map_[frame_id])->is_evictable_ && set_evictable) {
        (*frame_map_[frame_id])->is_evictable_ = set_evictable;
        curr_size_++;
    } 
    if ((*frame_map_[frame_id])->is_evictable_ && !set_evictable) {
        (*frame_map_[frame_id])->is_evictable_ = set_evictable;
        curr_size_--;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    auto iter = frame_map_.find(frame_id);

    if (iter == frame_map_.end() || !(*iter->second)->is_evictable_) {
        return;
    }

    if ((*iter->second)->k_distance_ < k_) {
        history_list_.remove(*iter->second);
        frame_map_.erase(frame_id);
        curr_size_--;
    } else if ((*iter->second)->k_distance_ >= k_) {
        // auto temp = std::move(*iter->second);
        frame_map_.erase(frame_id);
        cache_list_.remove(*iter->second);
        curr_size_--;
    }

}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
