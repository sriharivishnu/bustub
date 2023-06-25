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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : node_store_{num_frames}, replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  bool found = false;
  frame_id_t max_frame_id = -1;
  size_t max_k_distance;
  size_t lru_timestamp;

  std::scoped_lock latch_guard(latch_);

  for (const auto evictable_fid : evictable_) {
    const auto &node = node_store_[evictable_fid];
    size_t k_distance = node.history_.size() < k_ ? INF : node.history_.back() - node.history_.front();
    if (!found || (k_distance > max_k_distance) ||
        (k_distance == max_k_distance && node.history_.front() < lru_timestamp)) {
      found = true;
      max_frame_id = evictable_fid;
      max_k_distance = k_distance;
      lru_timestamp = node.history_.front();
    }
  }
  if (found) {
    // evict
    node_store_[max_frame_id].history_.clear();
    evictable_.erase(max_frame_id);
    if (frame_id != NULL) {
      *frame_id = max_frame_id;
    }
  }

  return found;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  if (frame_id < 0 || frame_id >= (frame_id_t)replacer_size_) {
    throw Exception("Invalid frame ID");
  }
  std::scoped_lock latch_guard(latch_);

  node_store_[frame_id].history_.push_back(current_timestamp_);
  if (node_store_[frame_id].history_.size() > k_) {
    node_store_[frame_id].history_.pop_front();
  }
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || frame_id >= (frame_id_t)replacer_size_) {
    throw Exception("Invalid frame ID");
  }

  std::scoped_lock latch_guard(latch_);

  bool is_evictable = evictable_.find(frame_id) != evictable_.end();
  if (is_evictable && !set_evictable) {
    evictable_.erase(frame_id);
  }
  if (!is_evictable && set_evictable) {
    evictable_.insert(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock latch_guard(latch_);

  if (evictable_.find(frame_id) == evictable_.end()) {
    return;
  }
  node_store_[frame_id].history_.clear();
}

auto LRUKReplacer::Size() -> size_t { return evictable_.size(); }

}  // namespace bustub
