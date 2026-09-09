# qdb

> 轻量级 GORM 封装 —— 一行连接串接入，泛型 DAO 覆盖常用 CRUD。

`qdb` 基于 [`gorm.io/gorm`](https://gorm.io) 与 [`kamioair/utils/qconfig`](https://github.com/kamioair/utils) 构建。
目标是：**用最少的样板代码，完成「数据库连接 + 表自动迁移 + 通用 CRUD」这一最常见链路**。

主要特性：

- 支持 **SQLite / MySQL / PostgreSQL / SQL Server** 四种数据库（SQLite 可指定 `journal_mode`）。
- 基于 `Setting` 结构体配置，自动从 `yaml` 读取并回写默认值。
- 泛型 `Dao[T]` 提供 `Create / Update / Save / Delete / Get* / Count` 等开箱即用方法。
- 内置 `DbBase` 基模型，自动维护 `id / create_at / updated_at / full_info` 字段。
- `NewDao[T]` 创建时按需 `AutoMigrate`，表结构与结构体保持一致。

---

## 1. 安装

```bash
go get github.com/kamioair/qdb
```

要求 **Go 1.20+**。本包会拉取对应版本的 `gorm.io/gorm` 及数据库驱动。

---

## 2. 快速开始

```go
package main

import (
    "fmt"
    "github.com/kamioair/qdb"
)

type User struct {
    qdb.DbBase            // 嵌入基础字段（id / create_at / updated_at / full_info）
    Name   string         `gorm:"column:name"`
    Age    int            `gorm:"column:age"`
}

func main() {
    // 1. 创建数据库连接（默认读取 ./config.yaml 的 DB 节点）
    db := qdb.NewDb(qdb.NewDefaultSetting("sqlite|./data.db&WAL"))

    // 2. 创建泛型 DAO（首次调用会自动建表）
    userDao := qdb.NewDao[User](db)

    // 3. CRUD
    _ = userDao.Create(&User{Name: "alice", Age: 30})

    u, _ := userDao.GetModel(1)
    fmt.Println(u)

    list, _ := userDao.GetAll()
    fmt.Println("total:", len(list))
}
```

---

## 3. 配置

### 3.1 连接串格式

```
<dbType>|<connStr>[&<extra>]
```

| dbType    | connStr 形式                                                                 | extra 含义              |
| ---       | ---                                                                          | ---                    |
| sqlite    | `./db/data.db`（文件路径，自动创建目录）                                      | journal_mode：`DELETE` / `MEMORY` / `WAL` / `OFF` |
| mysql     | `user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local` | 无                     |
| postgres  | `user:pass@host/dbname?...`                                                  | 无                     |
| sqlserver | `user:pass@host?database=dbname&encrypt=disable`                              | 无                     |

> SQLite 例：`sqlite|./data.db&WAL`
> MySQL 例：`mysql|root:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local`
> PostgreSQL 例：`postgres|postgres:123456@127.0.0.1:5432/test?sslmode=disable`
> SQL Server 例：`sqlserver|sa:123456@127.0.0.1?database=test&encrypt=disable`

### 3.2 `Setting` 结构

```go
type Setting struct {
    Connect string `comment:"数据库连接串..."`
    Config  struct {
        OpenLog                bool
        SkipDefaultTransaction bool
        NoLowerCase            bool
    }
    // sectionName、filePath 由 NewSetting 注入，不写入配置
    sectionName string
    filePath    string
}
```

| 字段 | 说明 |
| --- | --- |
| `Connect` | 数据库连接串，格式见上 |
| `Config.OpenLog` | 是否开启 GORM SQL 日志 |
| `Config.SkipDefaultTransaction` | 是否跳过默认事务（批量写入可显著提速） |
| `Config.NoLowerCase` | 是否关闭「表名/字段名转小写」命名策略 |
| `sectionName` / `filePath` | yaml 节点名与配置文件路径（私有字段，由构造函数赋值） |

### 3.3 构造方式

```go
// 方式一：默认配置（./config.yaml，节点 DB）
setting := qdb.NewDefaultSetting("sqlite|./data.db&OFF")

// 方式二：自定义配置文件 / 节点 / 参数
setting := qdb.NewSetting(
    "./conf/db.yaml",   // 配置文件
    "DB",               // 节点
    "mysql|root:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local",
    true,               // openLog
    true,               // skipDefaultTransaction
    false,              // noLowerCase
)
```

`NewDb(setting)` 调用流程：

1. 调用 `qconfig.LoadConfig` 读取 `filePath` 中 `sectionName` 节点，反序列化到 `Setting`。
2. 用读取后的连接串初始化数据库。
3. 若节点为空，将当前 `Setting` 回写到 yaml（方便二次编辑）。

---

## 4. `DB`：数据库对象

```go
type DB struct { /* ... */ }
```

| 方法 | 说明 |
| --- | --- |
| `NewDb(setting Setting) *DB` | 创建并初始化数据库，配置缺失 / 连接失败会 `panic` |
| `GetGormDB() *gorm.DB` | 取到底层 `*gorm.DB`，用于自定义查询、原生 SQL、事务等 |

> `NewDb` 失败会 `panic`，推荐在程序启动期调用一次；运行期不要重复创建。

---

## 5. `Dao[T]`：泛型数据访问对象

```go
type Dao[T any] struct { /* ... */ }

func NewDao[T any](db *DB) *Dao[T]
```

`NewDao[T]` 会按结构体名（`reflect.TypeOf(*m).Name()`）作为表名，**首次创建时自动执行 `AutoMigrate`**。若表已存在则跳过迁移。

> ⚠️ 命名策略默认 `SingularTable: true` 且 `NoLowerCase` 默认为 `true`（除非在 `Setting.Config.NoLowerCase` 中显式关闭）。因此表名直接使用结构体名，**不会**自动转小写或加复数 `s`。

### 5.1 方法一览

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `DB()` | `() *gorm.DB` | 取原生 `*gorm.DB`，做复杂查询 |
| `Create` | `(model *T) error` | 新增一条 |
| `CreateList` | `(list []T) error` | 事务批量新增 |
| `Update` | `(model *T) error` | 按主键更新（含零值字段） |
| `UpdateNonZero` | `(model *T) error` | 按主键更新（跳过零值字段，GORM 默认行为） |
| `UpdateList` | `(list []T) error` | 事务批量更新（含零值字段） |
| `Save` | `(model *T) error` | 「存在则更新、不存在则新增」（GORM `Save` 语义） |
| `SaveList` | `(list []T) error` | 事务批量 Save |
| `Delete` | `(id uint64) error` | 按主键删除 |
| `DeleteCondition` | `(condition string, args ...any) error` | 自定义条件删除 |
| `GetModel` | `(id uint64) (*T, error)` | 按主键查一条 |
| `CheckExist` | `(id uint64) bool` | 验证主键是否存在 |
| `GetList` | `(startId uint64, maxCount int) ([]T, error)` | `Limit(maxCount).Offset(int(startId))` 分页查询 |
| `GetAll` | `() ([]T, error)` | 查全表 |
| `GetCondition` | `(query, order string, args ...any) (*T, error)` | 条件查一条 |
| `GetConditions` | `(query, order string, count int, args ...any) ([]T, error)` | 条件查多条；`count=0` 表示不限制 |
| `GetCount` | `(query string, args ...any) int64` | 统计记录数 |

> **关于 `GetList(startId, maxCount)`**：参数 `startId` 实际是 SQL `OFFSET`，**不是起始主键**。若需按主键分页，请使用 `GetConditions` 自写条件。

### 5.2 `Update` / `UpdateNonZero` 的差异

`Update` 与 `UpdateList` 内部统一使用 `Select("*").Updates(...)`，**始终写入所有字段（包括零值）**。
当需要保留「零值不更新」的语义时，使用独立的 `UpdateNonZero` 方法。

```go
o := &Order{Price: 0}           // Price 为零值字段

// Update：包含零值字段（Price=0 会被写入数据库）
err := orderDao.Update(o)

// UpdateNonZero：跳过零值字段（GORM 默认行为）
err := orderDao.UpdateNonZero(o)
```

### 5.3 使用示例

```go
type Order struct {
    qdb.DbBase
    No    string  `gorm:"column:no;uniqueIndex"`
    Price float64 `gorm:"column:price"`
}

orderDao := qdb.NewDao[Order](db)

// 新增
_ = orderDao.Create(&Order{No: "A001", Price: 99.5})

// 按主键更新（包含零值）
o, _ := orderDao.GetModel(1)
o.Price = 0
_ = orderDao.Update(o)

// 条件查询
list, _ := orderDao.GetConditions("price > ?", "id desc", 50, 10)

// 统计
total := orderDao.GetCount("no LIKE ?", "A%")

// 删除
_ = orderDao.DeleteCondition("id IN ?", []uint64{1, 2, 3})
```

---

## 6. `DbBase`：基础模型

```go
type DbBase struct {
    Id        uint64         `gorm:"primaryKey;column:id"`
    CreateAt  qtime.DateTime `gorm:"column:create_at"`
    UpdatedAt qtime.DateTime `gorm:"index;column:updated_at"`
    FullInfo  string         `gorm:"column:full_info;type:text"`
}
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | `uint64` | 主键，自增 |
| `create_at` | `qtime.DateTime` | 创建时间（`BeforeCreate` 自动填充） |
| `updated_at` | `qtime.DateTime` | 更新时间，带索引；`BeforeCreate` / `BeforeUpdate` 自动填充 |
| `full_info` | `text` | 业务扩展字段（如 JSON、备注等） |

`DbBase` 已实现 `BeforeCreate` / `BeforeUpdate` 钩子，业务结构体直接嵌入即可：

```go
type Sample struct {
    qdb.DbBase
    Name string `gorm:"column:name"`
}
```

> `CreateAt` / `UpdatedAt` 类型为 `qtime.DateTime`（`kamioair/utils/qtime`）。若结构体未嵌入 `DbBase`，可手动使用 `qtime.NewDateTime(time.Now())`。

---

## 7. 注意事项

1. **失败语义**：`NewDb` 在配置解析失败、驱动不支持、连接错误时一律 `panic`。`NewDao[T]` 在 `AutoMigrate` 失败时返回 `nil`，调用前请判空。
2. **配置文件回写**：`NewDb` 会在配置节点缺失时把当前 `Setting` 回写到 yaml；如不希望覆盖，请保证节点存在。
3. **SQLite journal_mode**：`OFF` 会关闭事务日志，性能最佳但断电易损坏；推荐默认使用 `WAL`。
4. **驱动导入**：`go get github.com/kamioair/qdb` 会按需引入 `gorm.io/driver/{sqlite,postgres,mysql,sqlserver}`，无需手动安装。
5. **`NoLowerCase`**：默认 `true`（表名 / 字段名保持原大小写）。若希望 GORM 默认的小写命名，请在 `Setting.Config.NoLowerCase = false`。

---

## 8. License

MIT