#include "storage/index/b_plus_tree.h"

#include <cstddef>
#include <iostream>
#include <sstream>
#include <string>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub
{

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id,
                          BufferPoolManager* buffer_pool_manager,
                          const KeyComparator& comparator, int leaf_max_size,
                          int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id)
{
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  // In the original bpt, I fetch the header page
  // thus there's at least one page now
  auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  // reinterprete the data of the page into "HeaderPage"
  root_header_page->root_page_id_ = INVALID_PAGE_ID;
  // set the root_id to INVALID
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool
{
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  bool is_empty = root_header_page->root_page_id_ == INVALID_PAGE_ID;
  // Just check if the root_page_id is INVALID
  // usage to fetch a page:
  // fetch the page guard   ->   call the "As" function of the page guard
  // to reinterprete the data of the page as "BPlusTreePage"
  return is_empty;
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
auto BPLUSTREE_TYPE::GetValue(const KeyType& key,
                              std::vector<ValueType>* result, Transaction* txn)
    -> bool
{
  // Your code here
  Context ctx;
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  const auto* root_header_page =
      header_guard.template As<BPlusTreeHeaderPage>();
  page_id_t current_page_id = root_header_page->root_page_id_;
  if (current_page_id == INVALID_PAGE_ID)
  {
    return false;
  }
  ctx.root_page_id_ = current_page_id;
  auto read = bpm_->FetchPageRead(current_page_id);
  ctx.read_set_.push_back(std::move(read));
  FindLeafPage(key, Operation::Search, ctx);

  const auto* leaf_page = ctx.read_set_.back().template As<LeafPage>();

  int idx = BinaryFind(leaf_page, key);
  if (idx == -1 || comparator_(leaf_page->KeyAt(idx), key) != 0)
  {
    return false;
  }
  result->push_back(leaf_page->ValueAt(idx));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType& key, Operation op,
                                  Context& ctx)
{
  if (op == Operation::Search)
  {
    while (true)
    {
      const auto* tree_page = ctx.read_set_.back().template As<BPlusTreePage>();

      if (tree_page->IsLeafPage())
      {
        break;  // 找到叶子页，退出
      }

      const auto* internal_page =
          reinterpret_cast<const InternalPage*>(tree_page);
      int child_index = BinaryFind(internal_page, key);
      page_id_t child_page_id = internal_page->ValueAt(child_index);

      // 读取下一层 page 并加入 context 路径
      ReadPageGuard child_guard = bpm_->FetchPageRead(child_page_id);
      ctx.read_set_.push_back(std::move(child_guard));
    }
  }
  else if (op == Operation::Insert || op == Operation::Remove)
  {
    auto page = ctx.write_set_.back().As<BPlusTreePage>();
    while (!page->IsLeafPage())
    {
      auto internal = ctx.write_set_.back().As<InternalPage>();
      auto next_page_id = internal->ValueAt(BinaryFind(internal, key));
      ctx.write_set_.push_back(bpm_->FetchPageWrite(next_page_id));
      if (IsSafePage(ctx.write_set_.back().template As<BPlusTreePage>(), op,
                     false))
      {
        while (ctx.write_set_.size() > 1)
        {
          ctx.write_set_.pop_front();
        }
      }  // child is safe
      page = ctx.write_set_.back().template As<BPlusTreePage>();
    }
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafePage(const BPlusTreePage* tree_page, Operation op,
                                bool isRootPage) -> bool
{
  if (op == Operation::Search)
  {
    return true;
  }
  if (op == Operation::Insert)
  {
    if (tree_page->IsLeafPage())
    {
      return tree_page->GetSize() + 1 < tree_page->GetMaxSize();
    }
    return tree_page->GetSize() < tree_page->GetMaxSize();
  }
  if (op == Operation::Remove)
  {  // 删了之后仍安全
    if (isRootPage)
    {
      if (tree_page->IsLeafPage())
      {
        return tree_page->GetSize() > 1;
      }
      return tree_page->GetSize() > 2;
    }
    return tree_page->GetSize() > tree_page->GetMinSize();
  }
  return false;
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
auto BPLUSTREE_TYPE::Insert(const KeyType& key, const ValueType& value,
                            Transaction* txn) -> bool
{
  Context ctx;

  // 获取header page并保留写锁
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto* header_page = header_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_ = std::move(header_guard);

  // 空树情况处理
  if (header_page->root_page_id_ == INVALID_PAGE_ID)
  {
    auto new_leaf_guard = bpm_->NewPageGuarded(&ctx.root_page_id_);
    header_page->root_page_id_ = ctx.root_page_id_;

    auto* new_leaf = new_leaf_guard.template AsMut<LeafPage>();

    new_leaf->Init(leaf_max_size_);
    new_leaf->SetSize(1);
    new_leaf->SetKeyAt(0, key);
    new_leaf->SetValueAt(0, value);
    ctx.Drop();
    return true;
  }
  ctx.root_page_id_ = header_page->root_page_id_;
  // 获取root页并查找叶子节点
  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));
  if (IsSafePage(ctx.write_set_.back().As<BPlusTreePage>(), Operation::Insert,
                 true))
  {
    ctx.header_page_ = std::nullopt;  // unlock header_page
  }
  FindLeafPage(key, Operation::Insert, ctx);

  auto& leaf_guard = ctx.write_set_.back();
  auto* leaf = leaf_guard.template AsMut<LeafPage>();

  // 检查键是否已存在
  int idx = BinaryFind(leaf, key);
  if (idx != -1 && comparator_(leaf->KeyAt(idx), key) == 0)
  {
    return false;  // 键已存在
  }

  // 插入新键值
  int size = leaf->GetSize();
  idx++;
  leaf->IncreaseSize(1);
  // 移动元素腾出空间
  for (int i = size; i > idx; --i)
  {
    leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
    leaf->SetValueAt(i, leaf->ValueAt(i - 1));
  }
  leaf->SetKeyAt(idx, key);
  leaf->SetValueAt(idx, value);

  // 检查是否需要分裂
  if (leaf->GetSize() >= leaf->GetMaxSize())
  {
    auto [push_up_key, new_leaf_id] = SplitLeafPage(leaf, ctx);

    InsertIntoParent(push_up_key, new_leaf_id, ctx,
                     static_cast<int>(ctx.write_set_.size()) - 2);
  }

  ctx.Drop();  // 正常执行后释放锁
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage* leaf, Context& ctx)
    -> std::pair<KeyType, page_id_t>
{
  page_id_t new_page_id = 0;
  auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
  auto* new_leaf = new_page_guard.template AsMut<LeafPage>();

  leaf->SetNextPageId(new_page_guard.PageId());

  new_leaf->Init(leaf_max_size_);
  new_leaf->SetSize(leaf->GetSize() - leaf->GetMinSize());
  new_leaf->SetNextPageId(leaf->GetNextPageId());

  int total_size = leaf->GetSize();
  int move_start = leaf->GetMinSize();

  // 移动后半部分到新节点
  for (int i = move_start; i < total_size; i++)
  {
    new_leaf->SetKeyAt(i - move_start, leaf->KeyAt(i));
    new_leaf->SetValueAt(i - move_start, leaf->ValueAt(i));
  }

  leaf->SetSize(move_start);

  return {new_leaf->KeyAt(0), new_page_guard.PageId()};
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(const KeyType& key,
                                      page_id_t new_child_id, Context& ctx,
                                      int parent_index)
{
  // Case 1: 需要创建新根节点
  if (parent_index < 0)
  {
    // 创建新根节点
    page_id_t new_root_id = INVALID_PAGE_ID;
    auto new_root_guard = bpm_->NewPageGuarded(&new_root_id);
    auto* new_root = new_root_guard.template AsMut<InternalPage>();

    new_root->Init(internal_max_size_);
    new_root->SetSize(2);  // 新根节点有两个子节点

    // 设置新根节点的内容
    new_root->SetValueAt(
        0, ctx.write_set_[parent_index + 1].PageId());  // 左子节点
    new_root->SetKeyAt(1, key);                         // 分裂键
    new_root->SetValueAt(1, new_child_id);              // 右子节点

    auto header_page = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_id;

    // 更新上下文中的root_page_id
    // ctx.root_page_id_ = new_root_id;
    return;
  }

  // 获取父节点
  auto& parent_guard = ctx.write_set_[parent_index];
  auto* parent = parent_guard.template AsMut<InternalPage>();

  // Case 2: 父节点有空间，直接插入
  if (parent->GetSize() < parent->GetMaxSize())
  {
    // 找到插入位置
    int insert_pos = BinaryFind(parent, key) + 1;

    // 移动元素腾出空间
    parent->IncreaseSize(1);
    for (int i = parent->GetSize() - 1; i > insert_pos; --i)
    {
      parent->SetKeyAt(i, parent->KeyAt(i - 1));
      parent->SetValueAt(i, parent->ValueAt(i - 1));
    }

    // 插入新键值对
    parent->SetKeyAt(insert_pos, key);
    parent->SetValueAt(insert_pos, new_child_id);
    return;
  }

  // Case 3: 父节点已满，需要分裂
  page_id_t new_parent_id = INVALID_PAGE_ID;
  auto new_parent_guard = bpm_->NewPageGuarded(&new_parent_id);
  auto* new_parent = new_parent_guard.template AsMut<InternalPage>();
  new_parent->Init(internal_max_size_);

  // 计算分裂位置
  int split_pos = parent->GetMinSize();
  int insert_pos = BinaryFind(parent, key) + 1;
  new_parent->SetSize(parent->GetMaxSize() + 1 - split_pos);

  // 根据插入位置决定分裂方式
  if (insert_pos < split_pos)
  {
    // 新键插入到左半部分
    // 复制右半部分到新节点
    for (int i = split_pos; i < parent->GetSize(); ++i)
    {
      new_parent->SetKeyAt(i - split_pos + 1, parent->KeyAt(i));
      new_parent->SetValueAt(i - split_pos + 1, parent->ValueAt(i));
    }

    // 设置新节点的第一个子节点
    new_parent->SetKeyAt(0, parent->KeyAt(split_pos - 1));
    new_parent->SetValueAt(0, parent->ValueAt(split_pos - 1));

    // 在左节点中插入新键
    for (int i = split_pos - 1; i > insert_pos; --i)
    {
      parent->SetKeyAt(i, parent->KeyAt(i - 1));
      parent->SetValueAt(i, parent->ValueAt(i - 1));
    }
    parent->SetKeyAt(insert_pos, key);
    parent->SetValueAt(insert_pos, new_child_id);
  }
  else if (insert_pos == split_pos)
  {
    // 新键正好在分裂位置
    // 复制右半部分到新节点
    for (int i = split_pos; i < parent->GetSize(); ++i)
    {
      new_parent->SetKeyAt(i - split_pos + 1, parent->KeyAt(i));
      new_parent->SetValueAt(i - split_pos + 1, parent->ValueAt(i));
    }

    // 设置新节点的第一个子节点为新插入的子节点
    new_parent->SetValueAt(0, new_child_id);
    new_parent->SetKeyAt(0, key);
  }
  else
  {
    // 新键插入到右半部分
    // 复制右半部分到新节点（不包括新键）
    for (int i = split_pos; i < parent->GetSize(); ++i)
    {
      new_parent->SetKeyAt(i - split_pos, parent->KeyAt(i));
      new_parent->SetValueAt(i - split_pos, parent->ValueAt(i));
    }

    // // 设置新节点的第一个子节点
    new_parent->SetKeyAt(0, parent->KeyAt(split_pos));
    new_parent->SetValueAt(0, parent->ValueAt(split_pos));

    // 在新节点中插入新键
    insert_pos -= split_pos;
    for (int i = new_parent->GetSize(); i > insert_pos; --i)
    {
      new_parent->SetKeyAt(i, new_parent->KeyAt(i - 1));
      new_parent->SetValueAt(i, new_parent->ValueAt(i - 1));
    }
    new_parent->SetKeyAt(insert_pos, key);
    new_parent->SetValueAt(insert_pos, new_child_id);
  }

  // 更新节点大小
  parent->SetSize(split_pos);

  // 向上传递新节点的第一个键
  KeyType push_up_key = new_parent->KeyAt(0);

  // 递归处理父节点
  InsertIntoParent(push_up_key, new_parent_id, ctx, parent_index - 1);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType& key, Transaction* txn)
{
  Context ctx;
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  // 空树直接返回
  if (header_page->root_page_id_ == INVALID_PAGE_ID)
  {
    return;
  }

  // 获取根页面并加入写集合
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));

  // 如果根页面删除后不会下溢，可以释放头页面的锁
  if (IsSafePage(ctx.write_set_.back().As<BPlusTreePage>(), Operation::Remove,
                 true))
  {
    ctx.header_page_ = std::nullopt;
  }

  // 查找包含key的叶子页面
  FindLeafPage(key, Operation::Remove, ctx);
  auto& leaf_page_guard = ctx.write_set_.back();
  auto leaf_page = leaf_page_guard.AsMut<LeafPage>();

  // 查找key在叶子页面中的位置
  int pos = BinaryFind(leaf_page, key);
  if (pos == -1 || comparator_(leaf_page->KeyAt(pos), key) != 0)
  {
    // key不存在
    ctx.Drop();
    return;
  }

  // 执行删除操作 - 移动后续元素向前填补
  for (int i = pos + 1; i < leaf_page->GetSize(); ++i)
  {
    leaf_page->SetAt(i - 1, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
  }
  leaf_page->SetSize(leaf_page->GetSize() - 1);

  // 检查删除后是否下溢
  if (leaf_page->GetSize() >= leaf_page->GetMinSize())
  {
    ctx.Drop();
    return;
  }

  // 处理下溢情况
  if (ctx.IsRootPage(leaf_page_guard.PageId()))
  {
    // 如果是根节点且为空，将树置为空
    if (leaf_page->GetSize() == 0)
    {
      header_page->root_page_id_ = INVALID_PAGE_ID;
    }
    ctx.Drop();
    return;
  }

  // 获取父节点
  auto& parent_page_guard = ctx.write_set_[ctx.write_set_.size() - 2];
  auto parent_page = parent_page_guard.AsMut<InternalPage>();
  int index = BinaryFind(parent_page, key);  // 找到当前节点在父节点中的索引

  // 优先尝试与右兄弟合并或借元素
  if (index < parent_page->GetSize() - 1)
  {
    page_id_t right_page_id = parent_page->ValueAt(index + 1);
    auto right_page_guard = bpm_->FetchPageWrite(right_page_id);
    auto right_page = right_page_guard.AsMut<LeafPage>();

    // 检查是否可以合并
    if (leaf_page->GetSize() + right_page->GetSize() < leaf_page->GetMaxSize())
    {
      // 合并到当前节点
      int original_size = leaf_page->GetSize();
      leaf_page->SetSize(original_size + right_page->GetSize());
      for (int i = 0; i < right_page->GetSize(); ++i)
      {
        leaf_page->SetAt(original_size + i, right_page->KeyAt(i),
                         right_page->ValueAt(i));
      }
      leaf_page->SetNextPageId(right_page->GetNextPageId());

      RemoveFromParent(index + 1, ctx, ctx.write_set_.size() - 2);
    }
    else
    {
      // 从右兄弟借一个元素
      leaf_page->IncreaseSize(1);
      leaf_page->SetAt(leaf_page->GetSize() - 1, right_page->KeyAt(0),
                       right_page->ValueAt(0));

      // 移动右兄弟的元素
      for (int i = 1; i < right_page->GetSize(); ++i)
      {
        right_page->SetAt(i - 1, right_page->KeyAt(i), right_page->ValueAt(i));
      }
      right_page->IncreaseSize(-1);

      // 更新父节点的key
      parent_page->SetKeyAt(index + 1, right_page->KeyAt(0));
    }
  }
  else
  {
    // 没有右兄弟，尝试与左兄弟合并或借元素
    page_id_t left_page_id = parent_page->ValueAt(index - 1);
    auto left_page_guard = bpm_->FetchPageWrite(left_page_id);
    auto left_page = left_page_guard.AsMut<LeafPage>();

    // 检查是否可以合并
    if (left_page->GetSize() + leaf_page->GetSize() < left_page->GetMaxSize())
    {
      // 合并到左兄弟
      int original_size = left_page->GetSize();
      left_page->SetSize(original_size + leaf_page->GetSize());
      for (int i = 0; i < leaf_page->GetSize(); ++i)
      {
        left_page->SetAt(original_size + i, leaf_page->KeyAt(i),
                         leaf_page->ValueAt(i));
      }
      left_page->SetNextPageId(leaf_page->GetNextPageId());

      RemoveFromParent(index, ctx, ctx.write_set_.size() - 2);
    }
    else
    {
      // 从左兄弟借一个元素
      // 为借来的元素腾出空间
      for (int i = leaf_page->GetSize(); i > 0; --i)
      {
        leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
      }

      // 从左兄弟借最后一个元素
      leaf_page->SetAt(0, left_page->KeyAt(left_page->GetSize() - 1),
                       left_page->ValueAt(left_page->GetSize() - 1));
      leaf_page->IncreaseSize(1);
      left_page->IncreaseSize(-1);

      // 更新父节点的key
      parent_page->SetKeyAt(index, leaf_page->KeyAt(0));
    }
  }

  ctx.Drop();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromParent(int valueIndex, Context& ctx, int index)
{
  auto& page_guard = ctx.write_set_[index];
  auto page = page_guard.AsMut<InternalPage>();

  // 移动元素填补空缺
  for (int i = valueIndex + 1; i < page->GetSize(); ++i)
  {
    page->SetKeyAt(i - 1, page->KeyAt(i));
    page->SetValueAt(i - 1, page->ValueAt(i));
  }
  page->IncreaseSize(-1);

  // 检查是否下溢
  if (page->GetSize() >= page->GetMinSize())
  {
    return;
  }

  // 处理内部节点的下溢
  if (ctx.IsRootPage(page_guard.PageId()))
  {
    // 如果是根节点且只有一个孩子，降低树的高度
    if (page->GetSize() == 1)
    {
      auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = page->ValueAt(0);

      // 更新新根节点的父指针
      // auto new_root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
      // auto new_root = new_root_guard.AsMut<BPlusTreePage>();
    }
    return;
  }

  // 获取父节点
  auto& parent_page_guard = ctx.write_set_[index - 1];
  auto parent_page = parent_page_guard.AsMut<InternalPage>();
  int pos = parent_page->ValueIndex(page_guard.PageId());

  // 优先尝试与右兄弟合并或借元素
  if (pos < parent_page->GetSize() - 1)
  {
    page_id_t right_page_id = parent_page->ValueAt(pos + 1);
    auto right_page_guard = bpm_->FetchPageWrite(right_page_id);
    auto right_page = right_page_guard.AsMut<InternalPage>();

    // 检查是否可以合并
    if (page->GetSize() + right_page->GetSize() <= page->GetMaxSize())
    {
      // 合并到当前节点
      for (int i = 0; i < right_page->GetSize(); ++i)
      {
        page->SetKeyAt(page->GetSize(), right_page->KeyAt(i));
        page->SetValueAt(page->GetSize(), right_page->ValueAt(i));
        page->IncreaseSize(1);
      }

      RemoveFromParent(pos + 1, ctx, index - 1);
    }
    else
    {
      // 从右兄弟借一个元素
      page->SetKeyAt(page->GetSize(), right_page->KeyAt(0));
      page->SetValueAt(page->GetSize(), right_page->ValueAt(0));
      page->IncreaseSize(1);

      // 移动右兄弟的元素
      for (int i = 1; i < right_page->GetSize(); ++i)
      {
        right_page->SetKeyAt(i - 1, right_page->KeyAt(i));
        right_page->SetValueAt(i - 1, right_page->ValueAt(i));
      }
      right_page->IncreaseSize(-1);

      // 更新父节点的key
      parent_page->SetKeyAt(pos + 1, right_page->KeyAt(0));
    }
  }
  else
  {
    // 没有右兄弟，尝试与左兄弟合并或借元素
    page_id_t left_page_id = parent_page->ValueAt(pos - 1);
    auto left_page_guard = bpm_->FetchPageWrite(left_page_id);
    auto left_page = left_page_guard.AsMut<InternalPage>();

    // 检查是否可以合并
    if (left_page->GetSize() + page->GetSize() <= left_page->GetMaxSize())
    {
      // 合并到左兄弟
      for (int i = 0; i < page->GetSize(); ++i)
      {
        left_page->SetKeyAt(left_page->GetSize(), page->KeyAt(i));
        left_page->SetValueAt(left_page->GetSize(), page->ValueAt(i));
        left_page->IncreaseSize(1);
      }

      RemoveFromParent(pos, ctx, index - 1);
    }
    else
    {
      // 从左兄弟借一个元素
      // 为借来的元素腾出空间
      for (int i = page->GetSize(); i > 0; --i)
      {
        page->SetKeyAt(i, page->KeyAt(i - 1));
        page->SetValueAt(i, page->ValueAt(i - 1));
      }

      // 从左兄弟借最后一个元素
      page->SetKeyAt(0, left_page->KeyAt(left_page->GetSize() - 1));
      page->SetValueAt(0, left_page->ValueAt(left_page->GetSize() - 1));
      page->IncreaseSize(1);
      left_page->IncreaseSize(-1);

      // 更新父节点的key
      parent_page->SetKeyAt(pos, page->KeyAt(0));
    }
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const LeafPage* leaf_page, const KeyType& key)
    -> int
{
  int l = 0;
  int r = leaf_page->GetSize() - 1;
  while (l < r)
  {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page->KeyAt(mid), key) != 1)
    {
      l = mid;
    }
    else
    {
      r = mid - 1;
    }
  }

  if (r >= 0 && comparator_(leaf_page->KeyAt(r), key) == 1)
  {
    r = -1;
  }

  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const InternalPage* internal_page,
                                const KeyType& key) -> int
{
  int l = 1;
  int r = internal_page->GetSize() - 1;
  while (l < r)
  {
    int mid = (l + r + 1) >> 1;
    if (comparator_(internal_page->KeyAt(mid), key) != 1)
    {
      l = mid;
    }
    else
    {
      r = mid - 1;
    }
  }

  if (r == -1 || comparator_(internal_page->KeyAt(r), key) == 1)
  {
    r = 0;
  }

  return r;
}

/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE
// Just go left forever
{
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ ==
      INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard =
      bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  head_guard.Drop();

  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page->IsLeafPage())
  {
    int slot_num = 0;
    guard = bpm_->FetchPageRead(
        reinterpret_cast<const InternalPage*>(tmp_page)->ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  int slot_num = 0;
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
  }
  return End();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType& key) -> INDEXITERATOR_TYPE
{
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);

  if (head_guard.template As<BPlusTreeHeaderPage>()->root_page_id_ ==
      INVALID_PAGE_ID)
  {
    return End();
  }
  ReadPageGuard guard =
      bpm_->FetchPageRead(head_guard.As<BPlusTreeHeaderPage>()->root_page_id_);
  head_guard.Drop();
  auto tmp_page = guard.template As<BPlusTreePage>();
  while (!tmp_page->IsLeafPage())
  {
    auto internal = reinterpret_cast<const InternalPage*>(tmp_page);
    int slot_num = BinaryFind(internal, key);
    if (slot_num == -1)
    {
      return End();
    }
    guard = bpm_->FetchPageRead(
        reinterpret_cast<const InternalPage*>(tmp_page)->ValueAt(slot_num));
    tmp_page = guard.template As<BPlusTreePage>();
  }
  auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);

  int slot_num = BinaryFind(leaf_page, key);
  if (slot_num != -1)
  {
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE
{
  return INDEXITERATOR_TYPE(bpm_, -1, -1);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t
{
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = root_header_page->root_page_id_;
  return root_page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string& file_name,
                                    Transaction* txn)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key)
  {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string& file_name,
                                      Transaction* txn)
{
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input)
  {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction)
    {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager* bpm)
{
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage* page)
{
  if (page->IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId()
              << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++)
    {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  else
  {
    auto* internal = reinterpret_cast<const InternalPage*>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++)
    {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize())
      {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++)
    {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager* bpm, const std::string& outf)
{
  if (IsEmpty())
  {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage* page,
                             std::ofstream& out)
{
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage())
  {
    auto* leaf = reinterpret_cast<const LeafPage*>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize()
        << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++)
    {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID)
    {
      out << leaf_prefix << page_id << "   ->   " << leaf_prefix
          << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix
          << leaf->GetNextPageId() << "};\n";
    }
  }
  else
  {
    auto* inner = reinterpret_cast<const InternalPage*>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id
        << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize()
        << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++)
    {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      // if (i > 0) {
      out << inner->KeyAt(i) << "  " << inner->ValueAt(i);
      // } else {
      // out << inner  ->  ValueAt(0);
      // }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++)
    {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0)
      {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage())
        {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId()
              << " " << internal_prefix << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId()
          << "   ->   ";
      if (child_page->IsLeafPage())
      {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      }
      else
      {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string
{
  if (IsEmpty())
  {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id)
    -> PrintableBPlusTree
{
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage())
  {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++)
  {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub