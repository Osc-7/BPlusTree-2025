# Project5 : B+ tree in Database

> 
>
> 本次的Readme有关B+树以及数据库相关知识点的内容来自由TWL学长，并改编自其版本。

这是 "CS1963 (2024 - 2025 - 2) —— 程序设计与数据结构 II" 课程的第 5 个 project: 实现数据库中的 B+ 树。

在这次 project 中， 我们会为 bustub 关系型数据库编写 B+ 树索引。完成本次 project 不需要具备额外的数据库相关知识。

DDL : TBD


# 基础知识

在开始这个 project 之前， 我们需要了解一些基础知识。 由于课上已学习过 B 树与 B+ 树，这里没有对 B 树与 B+ 树进行介绍， 如有需要请查阅相关课程 PPT。如果你对 B+ 树进行操作后的结构有疑惑， 请在 https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html 网站上进行尝试。 此外， 这个博客的动图非常生动： https://zhuanlan.zhihu.com/p/149287061。

## 数据库的简单介绍(optional)

我们所说的数据库通常指数据库管理系统(DBMS, 即 Database Management System)。 我们可以简单理解为组织管理庞大数据的一个软件。

数据库可以分成很多类型， 这里我们所关心的 bustub 数据库是"关系型数据库"。 我们可以简单理解成， 这种数据库里存储的是一张张表格， 这些表格根据数据之间的关系建立起来。 例如， 下图就是一个表格示例。

<img src="https://notes.sjtu.edu.cn/uploads/upload_d74ed6ea471f51aa663eeb281bae90b9.png" width="500">

我们的表格通过遵守一定的格式存储在磁盘上。

对于数据库架构， 我从网络上找到了一个非常形象的图：(版权归"小林 coding"公众号所有， 该图为知名数据库 mysql 的架构示意图)

<img src="https://notes.sjtu.edu.cn/uploads/upload_2f43366c45fa12ba230efff3b21c3da4.png" width="800">

我们的数据库管理系统通常分为 Server 层和存储引擎层。 Server 层需要解决网络通信、 SQL 语句解析、 执行计划生成与优化等问题。 Server 层决定了用户输入的 SQL 查询语句是如何转化成优化后的执行计划。存储引擎层则负责数据的存储与提取。 不同存储引擎所使用的数据结构和实现方式可能并不相同。

如果你并不能看懂上图也没有关系。对于本次 project， 我们只需要关心 "存储引擎" 部分。下面我将介绍 bustub 数据库的存储引擎。

## 存储引擎与 B+ 树

我们的 bustub 数据库的存储引擎将数据存储在磁盘上， 实现数据的持久化。我们知道， 磁盘的一大特征便是空间大但访问速度非常慢， 因此， 我们希望能减少对于磁盘的交互访问 (以下称为磁盘 IO) 次数。

为什么我们采取 B+ 树作为这个存储引擎的数据结构？ 首先， 为了便于查询，我们需要给我们的表格建立 "目录"， 即选取表格中的某一列作为 "索引"。这样， 通过索引便可建立一个有序的数据结构(如二叉搜索树)。 我们查询时只需要先找到索引， 便可找到我们所需的数据行。 但二叉搜索树的深度太大， 导致对其进行查询（或插入删除）操作时磁盘 IO 次数太多。 因此， 我们可以考虑选取 B 树， B 树的深度往往远小于数据数量， 通常可维持在 3-4 层左右。 但 B 树每个结点都存储索引和数据行， 导致 B 树的单个节点所占空间太大。 另一方面， B 树不支持按照索引进行顺序查找。 因此， 我们可以将 B 树升级为 B+ 树， 只在叶子结点存储真正的数据行， 非叶子节点只存储索引。

<img src="https://notes.sjtu.edu.cn/uploads/upload_bfde29d73741b26103fce71094eae7e4.png" width="800">


实际上， 我们并不一定只创建唯一的一个 B+ 树。 试想我们需要存储每位同学的 `姓名、学号、性别、年龄、学院` 这些信息。 我们按照 `学号` 为索引构建了初始的 B+ 树， 叶子节点包含每位同学的记录信息。 但此时， 如果我们要频繁地对于 `年龄` 这一特征进行范围查找， 我们便希望所有数据按照 `年龄` 也是有序排列的。因此， 我们可以以 `年龄` 这一列为索引构建第二棵 B+ 树，这棵树的叶子节点存储的值为 `学号`(这样存储是因为数据记录只有一份)。 因此， 如果我们需要对 `年龄` 这一特征进行范围查找, 我们可以在第二棵树中进行搜索， 然后得到对应的 `学号` 值， 再回到第一棵树中搜索到对应的记录。

## 存储引擎与数据页(optional)

根据上方的表格图像，我们每行都存储了一条数据信息。但如果用户执行查询操作， 我们并不能以 "行" 为单位读取数据， 否则一次磁盘 IO 只能处理一行， 执行效率过低。 我们这里采取的策略是以 `page`（页）为单位进行磁盘 IO。 同时， 我们的索引结点也以 `page` 为基本单位进行存储， 即每个索引结点（中间结点）都对应一个 `page`。 我们可以简单地将一个 `page` 视为是固定大小的一块存储空间。 通过打包一系列数据进入同一个 page， 可以实现减少磁盘 IO 的效果。这里我们并不需要了解 `page` 的细节，与磁盘的 IO 操作已被封装为以下几个函数: `FetchPageRead`，`FetchPageWrite`。我会在之后详细说明这两个函数。 

Tips: 如果你听说过内存分页， 内存中的 `page` 和这里的 `page` 并不是同一概念， 请不要混淆。 另外， 如果你接触过文件系统， 我们的 bustub 数据库是建立于操作系统管理的文件系统之上的。 这里采取的策略是将数据表和对应的元数据存入一个或多个文件。

<img src="https://notes.sjtu.edu.cn/uploads/upload_5879015cd51b787ca781b64ac3e5e7b2.png" width="800">

### B+ 树加锁方法

B+ 树加锁采用 "螃蟹法则"。具体请见下图。 

<img src="https://notes.sjtu.edu.cn/uploads/upload_9c4c517643c2ab7eba276408e2233d8f.png" width="800">

<img src="https://notes.sjtu.edu.cn/uploads/upload_38c70fad05997aaf4ffb671539067d16.png" width="800">


这里我用大白话再解释一遍： 拿 `insert` 函数举例， 我们在搜索路径上每次都先拿 parent 结点的锁， 然后拿 child 结点的锁， 如果 child 结点是 "安全" 的， 就自上而下释放一路走下来所有 parent 结点的锁。（自上而下和自下而上都可以，但是自上而下能更快冲淡堵塞）

所谓的 "安全" 如何定义？ 只要 `child page` 插入时不满， 或者删除时至少半满，那就安全。 

进阶螃蟹法则（乐观锁）的意思就是， 对于写的线程仍然先拿读锁， 如果发现遇到了不安全的 `leaf page`, 可能引起上方的 `internal page` 也发生分裂， 那我立刻放弃继续执行乐观锁策略， 然后重新开始，依照普通的螃蟹法则进行写入的操作。

# 主体任务

请你修改 `src/include/storage/index/b_plus_tree.h` 和 `src/storage/index/b_plus_tree.cpp`, 实现 b+ 树的查找、插入和删除函数。 在此之后， 请你完善 b+ 树的查找、插入、删除函数， 使其线程安全。

# 熟悉项目代码

请跟着我一步一步熟悉那些本次 project 需要用到的代码文件。 让我们从 IO 的基本单元 `page` 开始。

## page

首先， 请看 `src/include/storage/page/page.h `:

我们的 page 类包含以下成员:

```cpp
  /** The actual data that is stored within a page. */
  // Usually this should be stored as `char data_[BUSTUB_PAGE_SIZE]{};`. But to
  // enable ASAN to detect page overflow, we store it as a ptr.
  char* data_;
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** The pin count of this page. */
  int pin_count_ = 0;
  /** True if the page is dirty, i.e. it is different from its corresponding
   * page on disk. */
  bool is_dirty_ = false;
  /** Page latch. */
  ReaderWriterLatch rwlatch_;
```

所以， 我们为每个 `page` 都实现了一个读写锁 `rwlatch_`, 我们之后的加锁是为 `page` 加锁。 其次， 我们的 `page` 包含一个 `char *` 区域存储 `page` 内部包含的数据。 `page_id` 是该 `page` 的唯一标识。其他成员你可以忽略。


## b_plus_tree_page

其次, 请看 `src/include/storage/page/b_plus_tree_page.h`:

我们的 B+ 树的 `page` 存储在上方原始 `page` 的 `data` 区域。 你可以认为， 上方的 `page` 包裹着这里的 `b_plus_tree_page`。

```cpp
/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 12 bytes in total):
 * ---------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
 * ---------------------------------------------------------
 */
class BPlusTreePage
{
  public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage& other) = delete;
  ~BPlusTreePage() = delete;

  auto IsLeafPage() const -> bool;
  void SetPageType(IndexPageType page_type);

  auto GetSize() const -> int;
  void SetSize(int size);
  void IncreaseSize(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;

  private:
  // Member variables, attributes that both internal and leaf page share
  IndexPageType page_type_;
  int size_;
  int max_size_;
};
```

在该类中， `GetSize` 用于得到该 `b_plus_tree_page` 当前存储的元素个数， `SetSize` 用于设置该 `b_plus_tree_page` 的元素个数， `IncreaseSize` 用于增加其元素个数。此外， `GetMaxSize` 可以得到该 `b_plus_tree_page` 允许存储的最大元素个数， `GetMinSize` 可以得到该 `b_plus_tree_page` 允许存储的最小元素个数。 这些成员函数会在插入和删除操作时派上用场。

此外， `IsLeafPage` 成员函数可以返回该 `Page` 是否为继承类 `BPlusTreeLeafPage`。

接下来请见 `src/include/storage/page/b_plus_tree_header_page.h`, 我们在这里定义了一个特殊的 `header page` 类型， 它存储着 B+ 树的根节点。 特殊定义一个 `header page` 有助于提升并发表现。

```cpp
class BPlusTreeHeaderPage
{
  public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeHeaderPage() = delete;
  BPlusTreeHeaderPage(const BPlusTreeHeaderPage& other) = delete;

  page_id_t root_page_id_;
};
```

请注意， 我们在 `page` 内部存储指向另一个 `page` 的 "指针" 时, 由于(不考虑缓存池的情况下) `page` 的真正位置在磁盘上， 因此我们采取存储 `page_id` 的策略， 而不是显式存储 `page` 指针。 `page_id` 可以唯一标识一个 `page`, 它的实现细节你可以忽略。 具体而言， 我们会得到一个函数 (即后文的 `NewPageGuarded`) 用于 "新建 `page`, 分配对应 `page id` 并返回该 `page_id`"。   

然后， 请见 `src/include/storage/page/b_plus_tree_internal_page.h`, `src/include/storage/page/b_plus_tree_leaf_page.h`:

```cpp
INDEX_TEMPLATE_ARGUMENTS
// 这是为了写模板简便定义的一个宏：
// #define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeInternalPage : public BPlusTreePage
{
  public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage& other) = delete;
  //...
  void Init(int max_size = INTERNAL_PAGE_SIZE);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  void SetKeyAt(int index, const KeyType& key);
  auto ValueIndex(const ValueType& value) const -> int;
  private:
  // Flexible array member for page data.
  MappingType array_[0];
  //MappingType 是这样定义的一个宏： #define MappingType std::pair<KeyType, ValueType>
};
```

```cpp
INDEX_TEMPLATE_ARGUMENTS
// 这是为了写模板简便定义的一个宏：
// #define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeLeafPage : public BPlusTreePage
{
  public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage& other) = delete;
  //...
  void Init(int max_size = LEAF_PAGE_SIZE);
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  void SetKeyAt(int index, const KeyType& key);
  void SetValueAt(int index, const ValueType & value);
  private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
  //MappingType 是这样定义的一个宏： #define MappingType std::pair<KeyType, ValueType>
};
```


我们的 `internal page` 和 `leaf page` 类都继承自 `BPlusTreePage` 类. 

`Internal page` 对应 B+ 树的内部结点， `leaf page` 对应 B+ 树的叶子结点。对于 `internal page`, 它存储着 n 个 索引 key 和 n + 1 个指向 `children page` 的指针。(由于我们以数组形式存储， 因此第一个数组元素对应的索引项无实际意义)  

对于 `LeafPage`, 它存储着 n 个 索引 key 和 n 个对应的数据行 ID。 这里的 `"KeyAt", "SetKeyAt", "ValueAt", "SetValueAt"` 可用于键值对的查询与更新， 会在 B+ 树的编写中用到。所有的叶子节点形成一个链表， 辅助函数 `GetNextPageId` 和 `SetNextPageId` 可用于维护这个链表。

`Init` 函数可用于手动刷新这个 `b_plus_tree_page`， 通常你不会手动调用这个成员函数， 但如果你的实现需要用到刷新 `b_plus_tree_page`, 你可以考虑调用它。

另外请注意， 这两个类继承自 `BPlusTreePage`， 因此别忘了可以使用 `BPlusTreePage` 的成员函数 (如 `GetSize`, `IncreaseSize`)！

## page_guard

然后， 请见 `src/include/storage/page/page_guard.h`。 我们在 `page.h` 中可以看到， 每个 `page` 都附带了一个读写锁。 为了避免遗忘释放锁这一操作， 我们使用 RAII 思想为 `page` 写了一个封装后的新类: `page_guard`。 

下面是 `page_guard` 的原型：

```cpp
// PageGuard 系列逻辑上的基类 BasicPageGuard
// 是创建新页时的返回类型
// 它不会自动上锁 + 解锁！
class BasicPageGuard
{
  public:
  BasicPageGuard() = default;
  BasicPageGuard(BufferPoolManager* bpm, Page* page) : bpm_(bpm), page_(page) {}

  BasicPageGuard(const BasicPageGuard&) = delete; 
  auto operator=(const BasicPageGuard&) -> BasicPageGuard& = delete;
  // 请注意， 我们删掉了拷贝构造和拷贝构造函数哦
  BasicPageGuard(BasicPageGuard&& that) noexcept;
  auto operator=(BasicPageGuard&& that) noexcept -> BasicPageGuard&;
  // 如果有需要， 请使用移动构造函数和移动赋值函数
  // (对于内部实现而言， 这样才能保证 PageGuard 对于 Pin_Count 和锁的正确管理)

  void Drop();
  // Drop 函数相当于手动析构
  // 请注意， 析构时会自动写入磁盘

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

// ReadPageGuard 会在构造时获取读锁， 析构时释放读锁
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
    // 我们存了一个 BasicPageGuard
    // 实现了逻辑上的继承
    bool unlock_guard{false};
};

// WritePageGuard 会在构造时获取写锁， 析构时释放写锁
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
    // 我们存了一个 BasicPageGuard
    // 实现了逻辑上的继承
    bool unlock_guard{false};
};
```

简而言之， `page_guard` 会在构造函数里获取锁， 在析构函数里释放锁。 我们对应设计了 `BasicPageGuard` 和它的逻辑上的派生类 `ReadPageGuard` 、 `WritePageGuard`. 

在这个 .h 文件里你还需要关注 `page_guard` 的 `As` 函数

```cpp
auto As() -> const T*
{
  return reinterpret_cast<const T*>(GetData());
}
```

另外， `AsMut` 函数即为 `As` 函数的 `非 const` 版本， 返回值为 `T * ` 而非 `const T * `:

```cpp
auto AsMut() -> T*
{
  return reinterpret_cast<T*>(GetDataMut());
}
```

它们可以获取 `page_guard` 封装起来的 `page` 的 `data` 区域, 将这块区域重新解释为某一类型。

此外， `Drop` 成员函数相当于手动调用析构函数： 它会释放对于 `page` 的所有权， 释放该 `page` 的锁， 并将内容写入磁盘。

另外， `UpgradeRead` 与 `UpgradeWrite` 可以将 `BasicPageGuard` 升级为 `ReadPageGuard` / `WritePageGuard`, 相当于获取并自动管理读锁 / 写锁。 如果你希望新建一个 `page` 之后立刻为它上锁， 这两个函数可能会派生用场。

```cpp
auto UpgradeRead() -> ReadPageGuard;
auto UpgradeWrite() -> WritePageGuard;
```

`PageId` 成员函数可以返回该 `page_guard` 对应的 `page_id`。 

```cpp
auto PageId() -> page_id_t { return guard_.PageId(); }
```

其他未提及的成员函数不是本次 project 必要的成员函数， 你可以忽略。

### page_guard 使用示例

请看我在 `src/storage/index/b_plus_tree.cpp` 给出的示例函数 `IsEmpty`:

```cpp
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const  ->  bool
{
  ReadPageGuard guard = bpm_ -> FetchPageRead(header_page_id_);
  auto root_header_page = guard.template As<BPlusTreeHeaderPage>();
  bool is_empty = root_header_page -> root_page_id_ == INVALID_PAGE_ID;
  // Just check if the root_page_id is INVALID
  // usage to fetch a page:
  // fetch the page guard   ->   call the "As" function of the page guard
  // to reinterprete the data of the page as "BPlusTreePage"
  return is_empty;
}
```

这里的 `guard.template As<BPlusTreeHeaderPage>();` 即为获取我们读到的 `ReadPageGuard` 封装的 `page` 的 `data` 区域， 将这块区域重新解释为 `BPlusTreeHeaderPage` 类型。 也就是， 我们获取了一个用来读的 `page`， 然后把这个 `page` 里面的数据解释为 `BPlusTreeHeaderPage`， 从而读取 `header page` 里的 `root_page_id` 信息， 检查是否是 `INVALID`.

上方有一个很奇怪的函数， 叫做 `bpm_ -> FetchPageRead`, 这是什么呢？ 请接着看。

## FetchPage by buffer pool manager

请见 `src/include/buffer/buffer_pool_manager.h `. 这份代码实际上是用于实现缓存池， 具体内容你不需要理解。

我们需要用到的函数有：

```cpp
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;
  auto NewPageGuarded(page_id_t* page_id, AccessType access_type = AccessType::Unknown) -> BasicPageGuard;
```

请你在本次 project 中将它们视为一个黑盒。

### Create New Page

`NewPageGuarded` 函数用于新建一个新的 `page`， 为其分配它的唯一标识 `PAGE ID` 并将该 `PAGE ID` 填入参数 `page_id` 中， 最后以 `page_guard` 类的形式返回它。


下面是一个使用 `NewPageGuarded` 函数的例子：

```cpp
page_id_t root_page_id;
// 这个临时变量用于保存此时分配的 page id

auto new_page_guard = bpm_ -> NewPageGuarded(&root_page_id);
// 调用 `NewPageGuarded`, 该函数会新建一个 Page， 并为它分配 ID
// 然后填入我们传入的 root_page_id 中
// 最后以 page_guard 的形式返回给我们
// 此时返回的是 page_guard 的基类 BasicPageGuard 
// 请注意， 此时该 page 未上锁。
// 如果你希望为它上锁， 可以使用上面提到的 UpgradeRead / UpgradeWrite
// 你也可以尝试使用其他方法为 BasicPageGuard 上锁
```

### Fetch Page

`bpm_ -> FetchPageRead` 和 `bpm_ -> FetchPageWrite` 用于通过磁盘 IO ， 根据 `page id` 得到一个用于读的 `ReadPageGuard` 或者一个用于写 `WritePageGuard`. 如果你好奇其中的细节(如缓存池)， 请私信我。 `ReadPageGuard` 会自动获取该 `page` 的读锁， 并在析构时释放读锁。 `WritePageGuard` 会自动获取该 `page` 的写锁， 并在析构时释放写锁。


下面是一些使用 `FetchPageRead / FetchPageWrite` 的例子：

```cpp
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType& key)  ->  INDEXITERATOR_TYPE
{
  ReadPageGuard head_guard = bpm_ -> FetchPageRead(header_page_id_);
    //读到 header page

  if (head_guard.template As<BPlusTreeHeaderPage>() -> root_page_id_ == INVALID_PAGE_ID)
  {
    return End();
  }
    //如果 header page 存的 root_page_id 是 INVALID, 说明树空， 返回 End()

  ReadPageGuard guard = bpm_ -> FetchPageRead(head_guard.As<BPlusTreeHeaderPage>() -> root_page_id_);
    //读到 root page, 即 B+ 树的根节点

  head_guard.Drop();
  //提前手动析构 header page

  auto tmp_page = guard.template As<BPlusTreePage>();
    //下面需要一步步寻找参数 key， 先把 guard 的 data 部分解释为 BPlusTreePage. 这一步实际上是我们这个 project 的惯例 : 拿到 page guard, 然后用 As 成员函数拿到 b_plus_tree_page 的指针。

  while (!tmp_page -> IsLeafPage())
  { 
    //如果不是叶子结点，我就一直找

    auto internal = reinterpret_cast<const InternalPage*>(tmp_page);
    //这里是内部结点， 那就把它 cast 成 InternalPage. InternalPage 是 BPlusTreeInternalPage 的别名。请注意， 只有我们的指针类型正确时候， 我们才能拿到这个类的数据成员和成员函数。

    int slot_num = BinaryFind(internal, key);
    //然后调用辅助函数 BinaryFind 在 page 内部二分查找这个 key， 找到该向下走哪个指针

    if (slot_num == -1)
    {
      return End();
    }
    //异常处理

    guard = bpm_ -> FetchPageRead(reinterpret_cast<const InternalPage*>(tmp_page) -> ValueAt(slot_num));
    //现在向下走， 根据上方得到的 page id 拿到新的 page guard。

    tmp_page = guard.template As<BPlusTreePage>();
    //然后再用相同方式把 page guard 的数据部分解释为 BPlusTreePage, 继续循环。
  }
  auto* leaf_page = reinterpret_cast<const LeafPage*>(tmp_page);
  //最后跳出循环， 说明找到了叶子结点。

  int slot_num = BinaryFind(leaf_page, key);
    //在叶子节点内部二分查找，找到对应的 key

  if (slot_num != -1)
  {
    //如果找到了， 构造对应迭代器。这个迭代器可以用于顺序访问所有数据。 本次 project 中不涉及迭代器的处理。
    return INDEXITERATOR_TYPE(bpm_, guard.PageId(), slot_num);
  } 
  return End();
}
```

### Write Page

你可能会问， 为什么没有一个函数把 `page` 写回磁盘呢？ 实际上， 只要你析构 `PageGuard`, 对应的 `page` 就会自动写回磁盘。

(这里有更多关于缓存池的细节， 如果感兴趣可以了解一下数据库缓存池)

## B+ 树核心代码

请见 `src/include/storage/index/b_plus_tree.h` 与 `src/storage/index/b_plus_tree.cpp`

请注意，你并不需要关心任何和 `BufferPoolManager`, `Transaction` 相关的数据成员和函数参数。 

你需要实现 B+ 树的查找 (`GetValue`)、插入（`Insert`）与删除 (`Remove`) 操作。 请注意， 整个 B+ 树存储于磁盘上， 因此每个结点都是以 `page` 形式存在， 需要使用 `FetchPageRead / FetchPageWrite` 函数将 `page` 从磁盘拿到内存中。


```cpp
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
     ->  bool
{
  //Your code here
  return true;
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
                            Transaction* txn)  ->  bool
{
  //Your code here
  return true;
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
  //Your code here
}
```

此外， 我们额外留心一下 B+ 树的构造函数：

```cpp
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
  WritePageGuard guard = bpm_ -> FetchPageWrite(header_page_id_);
  // In the original bpt, I fetch the header page
  // thus there's at least one page now
  auto root_header_page = guard.template AsMut<BPlusTreeHeaderPage>();
  // reinterprete the data of the page into "HeaderPage"
  root_header_page -> root_page_id_ = INVALID_PAGE_ID;
  // set the root_id to INVALID
}
```

这里需要注意的是， 我们传入了一个比较函数的函数对象， 如果你希望对 `key` 进行比较， 请使用这里的 `comparator_` 函数对象, 它的实现如下:

```cpp
  inline auto operator()(const GenericKey<KeySize>& lhs,
                         const GenericKey<KeySize>& rhs) const -> int
  {
    uint32_t column_count = key_schema_->GetColumnCount();

    for (uint32_t i = 0; i < column_count; i++)
    {
      Value lhs_value = (lhs.ToValue(key_schema_, i));
      Value rhs_value = (rhs.ToValue(key_schema_, i));

      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue)
      {
        return -1;
      }
      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue)
      {
        return 1;
      }
    }
    // equals
    return 0;
  }
```

如果你希望在测试时修改索引的值为某一整数， 你可以使用 `SetFromInteger`, 如

```cpp
  KeyType index_key;
  index_key.SetFromInteger(key);
  Remove(index_key, txn);
```


## Context

请见 `src/include/storage/index/b_plus_tree.h`.

`Context` 类可用于编写 B+ 树的螃蟹法则。 你可以使用它存储一条链上的锁， 也可以自己实现一个数据结构实现螃蟹法则。

```cpp
/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};
```

# 推荐的攻略

推荐你采取这样的路径进行编写：

+ 简单的插入操作。 在不考虑分裂的情况下编写 `insert` 函数。

+ 查找操作。 

实现以上二者之后请测试二者是否正确。

+ 简单的分裂操作。 请你编写 `insert` 函数， 考虑只有一个结点被分裂的情况。 (即此时不会递归分裂)

+ 递归的分裂操作。 请你继续编写 `insert` 函数， 考虑递归分类的情况。

到这里， 你应该可以通过 insert test。

+ 简单的删除操作。 此时不会发生合并或者借用。

+ 简单的合并 / 借用操作。 

+ 复杂的合并 / 借用操作。 此时会递归发生合并 / 借用。

到这里， 你应该可以通过 delete test。

+ 为以上函数增添并发组件， 使其线程安全。

# 测试方法

## 本地测试
==**建议使用Ubuntu22.04或20.04**==
### 使用Clion的集成功能（推荐）

如果你正确地将整个项目文件夹导入Clion，那么其应该将会自动识别其中的CMakelist，并识别出其中的ctest。

那么你只需将当前的配置设置为对应的ctest，运行即可（请确保工具链为Linux、WSL或者Macos环境的,以及有最基本的构建工具）。

若环境缺失，请先运行：

```shell
sudo build_support/packages.sh # Linux 环境请执行这个 仅限Ubuntu
build_support/packages.sh # macOS 可以直接这样执行
# Windows 环境请使用 wsl 或者虚拟机
```

你所需要运行的测试有：

```
b_plus_tree_insert_test
b_plus_tree_delete_test
b_plus_tree_contention_test
b_plus_tree_concurrent_test
```

### 命令行

请到该项目的根目录执行

```shell
sudo build_support/packages.sh # Linux 环境请执行这个 仅限Ubuntu
build_support/packages.sh # macOS 可以直接这样执行
# Windows 环境请使用 wsl 或者虚拟机

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
```

之后， 每次测试需要重新 `make`, 命令如下:

```shell
cd build #进入 build 目录， 如果已经在 build 目录请忽略
make b_plus_tree_insert_test b_plus_tree_delete_test b_plus_tree_contention_test b_plus_tree_concurrent_test -j$(nproc)
 #并行编译所有测试单元。 如果你暂时只想执行一部分测试程序， 请只 make 对应的 b_plus_tree_*_test。 
```

待编译好之后， 可以这样测试:

```shell
cd build #进入 build 目录， 如果已经在 build 目录请忽略
./test/b_plus_tree_insert_test
./test/b_plus_tree_delete_test
./test/b_plus_tree_contention_test
./test/b_plus_tree_concurrent_test
```

## 提交测试

由于本次项目过大， ACMOJ 不具备相关功能。 因此， 请通过本地测试后将代码压缩发给我， 我会尽快在我的本机上进行测试。

# 评分标准

``` python
./test/b_plus_tree_insert_test              45 分
./test/b_plus_tree_delete_test              45 分
------------------------------------------------
./test/b_plus_tree_contention_test          
./test/b_plus_tree_concurrent_test          5 分
------------------------------------------------
Code Review                                 10 分
```

满分上限为 __105__ 分，加满为止。 溢出 __100__ 分的部分抵消之前大作业、小作业所扣分数。

# 试一试你自己的数据库！(optional)

当你完成了对 B+ 树的编写后， 我们的数据库已经可以编译出接收 SQL 语句的 shell。

```shell
cd build #进入 build 目录， 若已在 build 目录请忽略
make -j$(nproc) shell
./bin/bustub-shell
```

之后， 你可以运行 `\dt` 来查看存储在数据库中的所有表格

```shell
bustub> \dt
+-----+----------------------------+------------------------------------------------------------------------------------------+
| oid | name                       | cols                                                                                     
+-----+----------------------------+------------------------------------------------------------------------------------------+
| 23  | test_2                     | (colA:INTEGER, colB:INTEGER, colC:INTEGER)                                               |
| 21  | test_simple_seq_2          | (col1:INTEGER, col2:INTEGER)                                                             |
...
| 12  | __mock_t1                  | (x:INTEGER, y:INTEGER, z:INTEGER)                                                        |
+-----+----------------------------+------------------------------------------------------------------------------------------+
```

你还可以编写各种 SQL 语句操作你的数据库

```shell
bustub> SELECT * FROM __mock_table_1;
+---------------------+---------------------+
| __mock_table_1.colA | __mock_table_1.colB |
+---------------------+---------------------+
| 0                   | 0                   |
| 1                   | 100                 |
| 2                   | 200                 |
| 3                   | 300                 |
...
| 98                  | 9800                |
| 99                  | 9900                |
+---------------------+---------------------+
```


如果你对数据库感兴趣， 强烈建议你在闲暇时刻学习 `CMU15445` 这门课， 并选择性阅读配套教材 "`Database Concept`"！


Acknowledgement : `CMU15445 Database System`. (https://15445.courses.cs.cmu.edu/spring2023/project2/).
