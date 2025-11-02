package qdb

import (
	"errors"
	"fmt"
	"github.com/kamioair/utils/qconfig"
	"github.com/kamioair/utils/qio"
	"github.com/kamioair/utils/qreflect"
	"github.com/kamioair/utils/qtime"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"reflect"
	"strings"
	"time"
)

// NewDb 创建DB
//
//	@param: sectionName: 配置节点名称
//	@param defaultConn 数据库连接串，为空使用默认值
//	         sqlite|./db/data.db&OFF
//	         sqlserver|用户名:密码@地址?database=数据库&encrypt=disable
//	         mysql|用户名:密码@tcp(127.0.0.1:3306)/数据库?charset=utf8mb4&parseTime=True&loc=Local
func NewDb(sectionName string, defaultConn string) *gorm.DB {
	cfg := initBaseConfig(defaultConn)
	err := qconfig.LoadConfig(cfg.filePath, sectionName, cfg)
	if err != nil {
		panic(err)
	}

	gc := gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
			NoLowerCase:   cfg.Config.NoLowerCase,
		},
		SkipDefaultTransaction: cfg.Config.SkipDefaultTransaction,
	}
	if cfg.Config.OpenLog {
		gc.Logger = logger.Default.LogMode(logger.Info)
	}
	sp := strings.Split(cfg.Connect, "|")

	// 创建数据库连接
	var db *gorm.DB
	switch sp[0] {
	case "sqlite":
		spp := strings.Split(sp[1], "&")
		// 创建数据库
		file := qio.GetFullPath(spp[0])
		if _, err := qio.CreateDirectory(file); err != nil {
			panic(err)
		}
		db, err = gorm.Open(sqlite.Open(file), &gc)
		if err != nil {
			panic(err)
		}
		// Journal模式
		//  DELETE：在事务提交后，删除journal文件
		//  MEMORY：在内存中生成journal文件，不写入磁盘
		//  WAL：使用WAL（Write-Ahead Logging）模式，将journal记录写入WAL文件中
		//  OFF：完全关闭journal模式，不记录任何日志消息
		if spp[1] != "" {
			db.Exec(fmt.Sprintf("PRAGMA journal_mode = %s;", spp[1]))
		}
	case "sqlserver":
		dsn := fmt.Sprintf("sqlserver://%s", sp[1])
		db, err = gorm.Open(sqlserver.Open(dsn), &gc)
		if err != nil {
			panic(err)
		}
	case "mysql":
		dsn := sp[1]
		db, err = gorm.Open(mysql.Open(dsn), &gc)
		if err != nil {
			panic(err)
		}
	case "postgres":
		dsn := sp[1]
		db, err = gorm.Open(postgres.Open(dsn), &gc)
		if err != nil {
			panic(err)
		}
	}
	if db == nil {
		panic(errors.New("unknown db type"))
	}
	return db
}

// 基础数据模型
type DbSimple struct {
	Id       uint64         `gorm:"primaryKey"` // 唯一号
	LastTime qtime.DateTime `gorm:"index"`      // 最后操作时间时间
}

type DbFull struct {
	Id       uint64         `gorm:"primaryKey"` // 唯一号
	LastTime qtime.DateTime `gorm:"index"`      // 最后操作时间时间
	FullInfo string         // 其他扩展内容
}

// DAO 通用数据访问对象
type Dao[T any] struct {
	db *gorm.DB
}

// NewDao 创建Dao
func NewDao[T any](db *gorm.DB) *Dao[T] {
	// 主动创建数据库
	m := new(T)
	name := reflect.TypeOf(*m).Name()
	if db.Migrator().HasTable(name) == false {
		err := db.AutoMigrate(m)
		if err != nil {
			return nil
		}
	}
	return &Dao[T]{db: db}
}

// DB 返回数据库连接
func (dao *Dao[T]) DB() *gorm.DB {
	return dao.db
}

// Create 新建一条记录
//
//	@param model 待新增实体
//	@return *T, error
func (dao *Dao[T]) Create(model *T) error {
	ref := qreflect.New(model)
	if ref.Get("LastTime") == "0001-01-01 00:00:00" {
		_ = ref.Set("LastTime", qtime.NewDateTime(time.Now()))
	}
	// 提交
	result := dao.DB().Create(model)
	return result.Error
}

// CreateList 创建一组列表
//
//	@param list 待新增列表
//	@return *T, error
func (dao *Dao[T]) CreateList(list []T) error {
	// 启动事务创建
	err := dao.DB().Transaction(func(tx *gorm.DB) error {
		for _, model := range list {
			ref := qreflect.New(model)
			if ref.Get("LastTime") == "0001-01-01 00:00:00" {
				_ = ref.Set("LastTime", qtime.NewDateTime(time.Now()))
			}
			if err := tx.Create(&model).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Update 修改一条记录
//
//	@param model 待更新实体
//	@return *T, error
func (dao *Dao[T]) Update(model *T) error {
	ref := qreflect.New(model)
	if ref.Get("LastTime") == "0001-01-01 00:00:00" {
		_ = ref.Set("LastTime", qtime.NewDateTime(time.Now()))
	}
	// 提交
	result := dao.DB().Model(model).Updates(model)
	if result.RowsAffected > 0 {
		return nil
	}
	if result.Error != nil {
		return result.Error
	}
	return errors.New("update record does not exist")
}

// UpdateList 修改一组记录
//
//	@param list 待更新列表
//	@return *T, error
func (dao *Dao[T]) UpdateList(list []T) error {
	err := dao.DB().Transaction(func(tx *gorm.DB) error {
		for _, model := range list {
			ref := qreflect.New(model)
			if ref.Get("LastTime") == "0001-01-01 00:00:00" {
				_ = ref.Set("LastTime", qtime.NewDateTime(time.Now()))
			}
			if err := tx.Updates(&model).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Save 修改一条记录（不存在则新增）
//
//	@param model 待保存实体
//	@return *T, error
func (dao *Dao[T]) Save(model *T) error {
	ref := qreflect.New(model)
	if ref.Get("LastTime") == "0001-01-01 00:00:00" {
		_ = ref.Set("LastTime", qtime.NewDateTime(time.Now()))
	}
	// 提交
	result := dao.DB().Save(model)
	return result.Error
}

// SaveList 修改一组记录（不存在则新增）
//
//	@param list 待保存列表
//	@return *T, error
func (dao *Dao[T]) SaveList(list []T) error {
	err := dao.DB().Transaction(func(tx *gorm.DB) error {
		for _, model := range list {
			ref := qreflect.New(model)
			if ref.Get("LastTime") == "0001-01-01 00:00:00" {
				_ = ref.Set("LastTime", qtime.NewDateTime(time.Now()))
			}
			if err := tx.Save(&model).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Delete 删除一条记录
//
//	@param id 唯一号
//	@return *T, error
func (dao *Dao[T]) Delete(id uint64) error {
	result := dao.DB().Where("id = ?", id).Delete(new(T))
	return result.Error
}

// DeleteCondition 自定义条件删除数据
//
//	@param condition 条件，如 id = ? 或 id IN (?) 等
//	@param args 条件参数，如 id, ids 等
//	@return error
func (dao *Dao[T]) DeleteCondition(condition string, args ...any) error {
	result := dao.DB().Where(condition, args...).Delete(new(T))
	return result.Error
}

// GetModel 获取一条记录
//
//	@param id 唯一号
//	@return *T, error
func (dao *Dao[T]) GetModel(id uint64) (*T, error) {
	// 创建空对象
	model := new(T)
	// 查询
	result := dao.DB().Where("id = ?", id).Find(model)
	// 如果异常或者未查询到任何数据
	if result.Error != nil || result.RowsAffected == 0 {
		return nil, result.Error
	}
	return model, nil
}

// CheckExist 验证数据是否存在
//
//	@return []*T, error
func (dao *Dao[T]) CheckExist(id uint64) bool {
	// 创建空对象
	model := new(T)
	// 查询
	result := dao.DB().Where("id = ?", id).Find(model)
	// 如果异常或者未查询到任何数据
	if result.Error != nil || result.RowsAffected == 0 {
		return false
	}
	return true
}

// GetList 查询一组列表
//
//	@param startId 其实id
//	@param maxCount 最大数量
//	@return []*T, error
func (dao *Dao[T]) GetList(startId uint64, maxCount int) ([]*T, error) {
	list := make([]*T, 0)
	// 查询
	result := dao.DB().Limit(maxCount).Offset(int(startId)).Find(&list)
	if result.Error != nil || result.RowsAffected == 0 {
		return list, result.Error
	}
	return list, nil
}

// GetAll 返回所有列表
//
//	@return []*T, error
func (dao *Dao[T]) GetAll() ([]*T, error) {
	list := make([]*T, 0)
	// 查询
	result := dao.DB().Find(&list)
	if result.Error != nil || result.RowsAffected == 0 {
		return list, result.Error
	}
	return list, nil
}

// GetCondition 条件查询一条记录
//
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param args 条件参数，如 id, ids 等
//	@return *T, error
func (dao *Dao[T]) GetCondition(query interface{}, args ...interface{}) (*T, error) {
	model := new(T)
	// 查询
	result := dao.DB().Where(query, args...).Find(model)
	if result.Error != nil || result.RowsAffected == 0 {
		return nil, result.Error
	}
	return model, nil
}

// GetConditionOrder 条件查询一条记录
//
//	@param order 排序，如 id asc, time desc
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param args 条件参数，如 id, ids 等
//	@return *T, error
func (dao *Dao[T]) GetConditionOrder(order string, query interface{}, args ...interface{}) (*T, error) {
	model := new(T)
	// 查询
	result := dao.DB().Order(order).Where(query, args...).Find(model)
	if result.Error != nil || result.RowsAffected == 0 {
		return nil, result.Error
	}
	return model, nil
}

// GetConditions 条件查询一组列表
//
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param args 条件参数，如 id, ids 等
//	@return []*T, error
func (dao *Dao[T]) GetConditions(query interface{}, args ...interface{}) ([]*T, error) {
	list := make([]*T, 0)
	// 查询
	result := dao.DB().Where(query, args...).Find(&list)
	if result.Error != nil || result.RowsAffected == 0 {
		return list, result.Error
	}
	return list, nil
}

// GetConditionsOrder 条件查询一组列表（自定义排序）
//
//	@param order 排序，如 id asc, time desc
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param args 条件参数，如 id, ids 等
//	@return []*T, error
func (dao *Dao[T]) GetConditionsOrder(order string, query interface{}, args ...interface{}) ([]*T, error) {
	list := make([]*T, 0)
	// 查询
	if order == "" {
		result := dao.DB().Where(query, args...).Find(&list)
		if result.Error != nil || result.RowsAffected == 0 {
			return list, result.Error
		}
	} else {
		result := dao.DB().Order(order).Where(query, args...).Find(&list)
		if result.Error != nil || result.RowsAffected == 0 {
			return list, result.Error
		}
	}
	return list, nil
}

// GetConditionsLimit 条件查询一组列表（限制数量）
//
//	@param maxCount 最大数量
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param args 条件参数，如 id, ids 等
//	@return []*T, error
func (dao *Dao[T]) GetConditionsLimit(maxCount int, query interface{}, args ...interface{}) ([]*T, error) {
	list := make([]*T, 0)
	// 查询
	if maxCount > 0 {
		result := dao.DB().Where(query, args...).Limit(maxCount).Find(&list)
		if result.Error != nil || result.RowsAffected == 0 {
			return list, result.Error
		}
	} else {
		result := dao.DB().Where(query, args...).Find(&list)
		if result.Error != nil || result.RowsAffected == 0 {
			return list, result.Error
		}
	}
	return list, nil
}

// GetCount 获取总记录数
//
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param args 条件参数，如 id, ids 等
//	@return int64
func (dao *Dao[T]) GetCount(query interface{}, args ...interface{}) int64 {
	// 创建空对象
	model := new(T)
	// 查询
	var count int64
	dao.DB().Model(model).Where(query, args...).Count(&count)
	return count
}
