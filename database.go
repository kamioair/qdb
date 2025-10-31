package qdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kamioair/utils/qconfig"
	"github.com/kamioair/utils/qio"
	"github.com/kamioair/utils/qreflect"
	"github.com/kamioair/utils/qtime"
	"github.com/spf13/viper"
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

// 需要从核心包导入配置，为了避免循环依赖，这里重新定义配置接口
type BaseConfig interface {
	GetModuleInfo() (Name string, Desc string, Version string)
}

var baseCfg BaseConfig

// SetBaseConfig 设置基础配置，由主程序调用
func SetBaseConfig(cfg BaseConfig) {
	baseCfg = cfg
}

type setting struct {
	Connect string
	Config  config
}

type config struct {
	OpenLog                bool
	SkipDefaultTransaction bool
	NoLowerCase            bool
}

// NewDb 创建DB
func NewDb(section string) *gorm.DB {
	if baseCfg == nil {
		panic(errors.New("base config not set, please call SetBaseConfig first"))
	}

	setting := setting{}
	moduleName, _, _ := baseCfg.GetModuleInfo()
	cfg := viper.Get(fmt.Sprintf("%s.%s", moduleName, section))
	if cfg == nil {
		cfg = viper.Get(section)
		if cfg == nil {
			// 写入默认值
			setting.Connect = "sqlite|./db/data.db&OFF"
			setting.Config.SkipDefaultTransaction = true
			setting.Config.NoLowerCase = true
			// 生成配置内容字符串
			configModule := map[string]any{}
			configModule[section] = setting
			newCfg := ""
			newCfg += fmt.Sprintf("############################### %s DB Config ###############################\n", section)
			newCfg += fmt.Sprintf("# %s 数据库配置\n", section)
			newCfg += "# connect：数据库连接串\n"
			newCfg += "#   sqlite|./db/data.db&OFF  OFF=(DELETE/MEMORY/WAL/OFF)\n"
			newCfg += "#   sqlserver|用户名:密码@地址?database=数据库&encrypt=disable\n"
			newCfg += "#   mysql|用户名:密码@tcp(127.0.0.1:3306)/数据库?charset=utf8mb4&parseTime=True&loc=Local\n"
			newCfg += "# 其他设置\n"
			newCfg += "#   openLog：是否打开调试日志\n"
			newCfg += "#   skipDefaultTransaction：是否跳过默认事务\n"
			newCfg += "#   noLowerCase：是否不将结构体名和字段名转换为小写字母的形式\n"
			newCfg += qconfig.ToYAML(configModule, 0, []string{""})

			// 尝试检测是否有变化，如果有则更新文件
			qconfig.TrySave("./config.yaml", newCfg)
		} else {
			js, err := json.Marshal(cfg)
			if err == nil {
				err = json.Unmarshal(js, &setting)
				if err != nil {
					return nil
				}
			}
		}
	} else {
		js, err := json.Marshal(cfg)
		if err == nil {
			err = json.Unmarshal(js, &setting)
			if err != nil {
				return nil
			}
		}
	}
	gc := gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
			NoLowerCase:   setting.Config.NoLowerCase,
		},
		SkipDefaultTransaction: setting.Config.SkipDefaultTransaction,
	}
	if setting.Config.OpenLog {
		gc.Logger = logger.Default.LogMode(logger.Info)
	}
	sp := strings.Split(setting.Connect, "|")

	// 创建数据库连接
	var db *gorm.DB
	var err error
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
	Summary  string         // 摘要
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
func (dao *Dao[T]) Delete(id uint64) error {
	result := dao.DB().Where("id = ?", id).Delete(new(T))
	return result.Error
}

// DeleteCondition 自定义条件删除数据
func (dao *Dao[T]) DeleteCondition(condition string, args ...any) error {
	result := dao.DB().Where(condition, args...).Delete(new(T))
	return result.Error
}

// GetModel 获取一条记录
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
func (dao *Dao[T]) GetList(startId uint64, maxCount int) ([]T, error) {
	list := make([]T, 0)
	// 查询
	result := dao.DB().Limit(int(maxCount)).Offset(int(startId)).Find(&list)
	if result.Error != nil || result.RowsAffected == 0 {
		return list, result.Error
	}
	return list, nil
}

// GetAll 返回所有列表
func (dao *Dao[T]) GetAll() ([]T, error) {
	list := make([]T, 0)
	// 查询
	result := dao.DB().Find(&list)
	if result.Error != nil || result.RowsAffected == 0 {
		return list, result.Error
	}
	return list, nil
}

// GetCondition 条件查询一条记录
func (dao *Dao[T]) GetCondition(query interface{}, args ...interface{}) (*T, error) {
	model := new(T)
	// 查询
	result := dao.DB().Where(query, args...).Find(model)
	if result.Error != nil || result.RowsAffected == 0 {
		return nil, result.Error
	}
	return model, nil
}

// GetConditions 条件查询一组列表
func (dao *Dao[T]) GetConditions(query interface{}, args ...interface{}) ([]T, error) {
	list := make([]T, 0)
	// 查询
	result := dao.DB().Where(query, args...).Find(&list)
	if result.Error != nil || result.RowsAffected == 0 {
		return list, result.Error
	}
	return list, nil
}

// GetConditionsLimit 条件查询一组列表（限制数量）
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
func (dao *Dao[T]) GetCount(query interface{}, args ...interface{}) int64 {
	// 创建空对象
	model := new(T)
	// 查询
	var count int64
	dao.DB().Model(model).Where(query, args...).Count(&count)
	return count
}
