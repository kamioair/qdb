package qdb

import (
	"errors"
	"github.com/kamioair/utils/qtime"
	"gorm.io/gorm"
	"reflect"
	"time"
)

// DbBase 基础数据模型
type DbBase struct {
	Id        uint64         `gorm:"primaryKey;column:id"`       // 唯一号
	CreateAt  qtime.DateTime `gorm:"column:create_at"`           // 创建时间
	UpdatedAt qtime.DateTime `gorm:"index;column:updated_at"`    // 最后操作时间
	FullInfo  string         `gorm:"column:full_info;type:text"` // 其他扩展内容
}

// BeforeCreate 创建前钩子 - 自动设置时间
func (b *DbBase) BeforeCreate(tx *gorm.DB) error {
	now := qtime.NewDateTime(time.Now())
	// 如果 CreateAt 为零值，则设置
	if b.CreateAt == 0 {
		b.CreateAt = now
	}
	// 如果 UpdatedAt 为零值，则设置
	if b.UpdatedAt == 0 {
		b.UpdatedAt = now
	}
	return nil
}

// BeforeUpdate 更新前钩子 - 自动更新 UpdatedAt
func (b *DbBase) BeforeUpdate(tx *gorm.DB) error {
	// 只更新 UpdatedAt，不修改 CreateAt
	if b.UpdatedAt == 0 {
		b.UpdatedAt = qtime.NewDateTime(time.Now())
	}
	return nil
}

// Dao 通用数据访问对象
type Dao[T any] struct {
	db *gorm.DB
}

// NewDao 创建Dao
func NewDao[T any](db *DB) *Dao[T] {
	dao := &Dao[T]{db: db.GetGormDB()}
	// 主动创建数据库
	m := new(T)
	name := reflect.TypeOf(*m).Name()
	if dao.db.Migrator().HasTable(name) == false {
		err := dao.db.AutoMigrate(m)
		if err != nil {
			return nil
		}
	}
	return dao
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
			if err := tx.Create(&model).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Update 修改一条记录（更新所有字段，包括零值）
//
//	@param model 待更新实体
//	@return error
func (dao *Dao[T]) Update(model *T) error {
	result := dao.DB().Model(model).Select("*").Updates(model)
	if result.RowsAffected > 0 {
		return nil
	}
	if result.Error != nil {
		return result.Error
	}
	return errors.New("update record does not exist")
}

// UpdateNonZero 修改一条记录（仅更新非零值字段）
//
//	@param model 待更新实体
//	@return error
func (dao *Dao[T]) UpdateNonZero(model *T) error {
	result := dao.DB().Model(model).Updates(model)
	if result.RowsAffected > 0 {
		return nil
	}
	if result.Error != nil {
		return result.Error
	}
	return errors.New("update record does not exist")
}

// UpdateList 修改一组记录（更新所有字段，包括零值）
//
//	@param list 待更新列表
//	@return error
func (dao *Dao[T]) UpdateList(list []T) error {
	if len(list) == 0 {
		return nil
	}

	return dao.DB().Transaction(func(tx *gorm.DB) error {
		for _, model := range list {
			// 更新所有字段，包括零值
			if err := tx.Model(&model).Select("*").Updates(&model).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// Save 修改一条记录（不存在则新增）
//
//	@param model 待保存实（注：这个方法一定会更新所有字段）
//	@return *T, error
func (dao *Dao[T]) Save(model *T) error {
	// 提交
	result := dao.DB().Save(model)
	return result.Error
}

// SaveList 修改一组记录（不存在则新增）
//
//	@param list 待保存列表（注：这个方法一定会更新所有字段）
//	@return *T, error
func (dao *Dao[T]) SaveList(list []T) error {
	err := dao.DB().Transaction(func(tx *gorm.DB) error {
		for _, model := range list {
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
func (dao *Dao[T]) GetList(startId uint64, maxCount int) ([]T, error) {
	list := make([]T, 0)
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
//
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param order 排序，为空表示不排序，如 id asc, time desc
//	@param args 条件参数，如 id, ids 等
//	@return *T, error
func (dao *Dao[T]) GetCondition(query interface{}, order string, args ...interface{}) (*T, error) {
	model := new(T)
	// 查询
	if order == "" {
		result := dao.DB().Where(query, args...).Find(model)
		if result.Error != nil || result.RowsAffected == 0 {
			return nil, result.Error
		}
	} else {
		result := dao.DB().Order(order).Where(query, args...).Find(model)
		if result.Error != nil || result.RowsAffected == 0 {
			return nil, result.Error
		}
	}

	return model, nil
}

// GetConditions 条件查询一组列表
//
//	@param query 条件，如 id = ? 或 id IN (?) 等
//	@param order 排序，为空表示不排序，如 id asc, time desc
//	@param count 最大数量，0表示全部
//	@param args 条件参数，如 id, ids 等
//	@return []*T, error
func (dao *Dao[T]) GetConditions(query interface{}, order string, count int, args ...interface{}) ([]T, error) {
	list := make([]T, 0)
	// 查询
	if order == "" {
		if count == 0 {
			result := dao.DB().Where(query, args...).Find(&list)
			if result.Error != nil || result.RowsAffected == 0 {
				return list, result.Error
			}
		} else {
			result := dao.DB().Where(query, args...).Limit(count).Find(&list)
			if result.Error != nil || result.RowsAffected == 0 {
				return list, result.Error
			}
		}
	} else {
		if count == 0 {
			result := dao.DB().Order(order).Where(query, args...).Find(&list)
			if result.Error != nil || result.RowsAffected == 0 {
				return list, result.Error
			}
		} else {
			result := dao.DB().Order(order).Where(query, args...).Limit(count).Find(&list)
			if result.Error != nil || result.RowsAffected == 0 {
				return list, result.Error
			}
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
