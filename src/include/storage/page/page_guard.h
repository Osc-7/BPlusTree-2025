#pragma once

#include "storage/page/page.h"

namespace bustub
{

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

class BasicPageGuard
{
  public:
  BasicPageGuard() = default;

  BasicPageGuard(BufferPoolManager* bpm, Page* page) : bpm_(bpm), page_(page) {}

  BasicPageGuard(const BasicPageGuard&) = delete;

  auto operator=(const BasicPageGuard&) -> BasicPageGuard& = delete; 

  BasicPageGuard(BasicPageGuard&& that) noexcept;

  void Drop();

  auto operator=(BasicPageGuard&& that) noexcept -> BasicPageGuard&;

  ~BasicPageGuard();

  auto UpgradeRead() -> ReadPageGuard;

  auto UpgradeWrite() -> WritePageGuard;

  auto PageId() -> page_id_t { return page_->GetPageId(); }

  auto GetData() -> const char* { return page_->GetData(); }

  template <class T>
  auto As() -> const T*
  {
    return reinterpret_cast<const T*>(GetData());
  }

  auto GetDataMut() -> char*
  {
    is_dirty_ = true;
    return page_->GetData();
  }

  template <class T>
  auto AsMut() -> T*
  {
    return reinterpret_cast<T*>(GetDataMut());
  }

  private:
  friend class ReadPageGuard;
  friend class WritePageGuard;

  BufferPoolManager* bpm_{nullptr};
  Page* page_{nullptr};
  bool is_dirty_{false};
};

class ReadPageGuard
{
  public:
  ReadPageGuard() = default;
  ReadPageGuard(BufferPoolManager* bpm, Page* page);
  ReadPageGuard(const ReadPageGuard&) = delete;
  auto operator=(const ReadPageGuard&) -> ReadPageGuard& = delete;

  ReadPageGuard(ReadPageGuard&& that) noexcept;

  auto operator=(ReadPageGuard&& that) noexcept -> ReadPageGuard&;

  void Drop();

  ~ReadPageGuard();

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char* { return guard_.GetData(); }

  template <class T>
  auto As() -> const T*
  {
    return guard_.As<T>();
  }

  private:
  BasicPageGuard guard_;
  bool unlock_guard{false};
};

class WritePageGuard
{
  public:
  WritePageGuard() = default;
  WritePageGuard(BufferPoolManager* bpm, Page* page);
  WritePageGuard(const WritePageGuard&) = delete;
  auto operator=(const WritePageGuard&) -> WritePageGuard& = delete;

  WritePageGuard(WritePageGuard&& that) noexcept;

  auto operator=(WritePageGuard&& that) noexcept -> WritePageGuard&;

  void Drop();

  ~WritePageGuard();

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char* { return guard_.GetData(); }

  template <class T>
  auto As() -> const T*
  {
    return guard_.As<T>();
  }

  auto GetDataMut() -> char* { return guard_.GetDataMut(); }

  template <class T>
  auto AsMut() -> T*
  {
    return guard_.AsMut<T>();
  }

  private:
  BasicPageGuard guard_;
  bool unlock_guard{false};
};

}  // namespace bustub
