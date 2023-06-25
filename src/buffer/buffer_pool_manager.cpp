//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock latch_guard{latch_};
  frame_id_t frame_id;
  if (!FindAndEvictFrame(&frame_id)) {
    return nullptr;
  }

  // pin the frame in the replacer
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);

  // create the new page
  page_id_t new_page_id = AllocatePage();
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 1;
  page_table_[new_page_id] = frame_id;
  pages_[frame_id].ResetMemory();

  // return the result
  *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::scoped_lock latch_guard{latch_};
  if (!page_table_.count(page_id)) {
    frame_id_t frame_id;
    if (!FindAndEvictFrame(&frame_id)) {
      return nullptr;
    }

    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 0;
    page_table_[page_id] = frame_id;
  }
  pages_[page_table_[page_id]].pin_count_++;

  replacer_->SetEvictable(page_table_[page_id], false);
  replacer_->RecordAccess(page_table_[page_id]);

  return &pages_[page_table_[page_id]];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::scoped_lock latch_guard{latch_};
  if (!page_table_.count(page_id)) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];

  if (pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  --pages_[frame_id].pin_count_;

  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock latch_guard{latch_};

  if (!page_table_.count(page_id)) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];

  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock latch_guard{latch_};

  for (const auto &[page_id, frame_id] : page_table_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock latch_guard{latch_};
  if (!page_table_.count(page_id)) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() >= 1) {
    return false;
  }

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }

  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();

  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id, AccessType::Unknown);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id, AccessType::Unknown);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id, AccessType::Unknown);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FindAndEvictFrame(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(frame_id)) {
      return false;
    }
    Page *frame = &pages_[*frame_id];
    page_table_.erase(frame->GetPageId());
    if (frame->IsDirty()) {
      disk_manager_->WritePage(frame->GetPageId(), pages_[*frame_id].GetData());
      frame->is_dirty_ = false;
    }

    frame->page_id_ = INVALID_PAGE_ID;
    // TODO(sriharivishnu): is this necessary?
    frame->ResetMemory();
  }
  return true;
}
}  // namespace bustub
