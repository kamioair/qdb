package qdb

import (
	"errors"
	"fmt"
	"github.com/kamioair/utils/qconfig"
	"github.com/kamioair/utils/qio"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"strings"
)

// DB 数据库对象
type DB struct {
	db      *gorm.DB
	setting *Setting
}

// NewDb 创建DB
//
//	@param: setting: 数据库配置，默认 qdb.NewDefaultSetting("sqlite|./db/data.db&OFF")
func NewDb(setting Setting) *DB {
	db := &DB{
		setting: &setting,
	}

	// 如果节点为空，则读取配置
	err := qconfig.LoadConfig(db.setting.filePath, db.setting.sectionName, db.setting)
	if err != nil {
		panic(err)
	}

	// 初始化数据库
	db.initDB()

	// 如果节点为空，则保存配置
	db.saveConfig()

	return db
}

// GetGormDB 获取GormDB对象
func (db *DB) GetGormDB() *gorm.DB {
	return db.db
}

func (db *DB) saveConfig() {
	save := qconfig.SaveContent{}
	save.Add(db.setting.sectionName, "DB Config", db.setting)
	_ = qconfig.SaveConfig(db.setting.filePath, save)
}

func (db *DB) initDB() {
	var err error
	gc := gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
			NoLowerCase:   db.setting.Config.NoLowerCase,
		},
		SkipDefaultTransaction: db.setting.Config.SkipDefaultTransaction,
	}
	if db.setting.Config.OpenLog {
		gc.Logger = logger.Default.LogMode(logger.Info)
	}
	sp := strings.Split(db.setting.Connect, "|")

	// 创建数据库连接
	var gormDB *gorm.DB
	switch sp[0] {
	case "sqlite":
		spp := strings.Split(sp[1], "&")
		// 创建数据库
		file := qio.GetFullPath(spp[0])
		if _, err := qio.CreateDirectory(file); err != nil {
			panic(err)
		}
		gormDB, err = gorm.Open(sqlite.Open(file), &gc)
		if err != nil {
			panic(err)
		}
		// Journal模式
		//  DELETE：在事务提交后，删除journal文件
		//  MEMORY：在内存中生成journal文件，不写入磁盘
		//  WAL：使用WAL（Write-Ahead Logging）模式，将journal记录写入WAL文件中
		//  OFF：完全关闭journal模式，不记录任何日志消息
		if spp[1] != "" {
			gormDB.Exec(fmt.Sprintf("PRAGMA journal_mode = %s;", spp[1]))
		}
	case "sqlserver":
		dsn := fmt.Sprintf("sqlserver://%s", sp[1])
		gormDB, err = gorm.Open(sqlserver.Open(dsn), &gc)
		if err != nil {
			panic(err)
		}
	case "mysql":
		dsn := sp[1]
		gormDB, err = gorm.Open(mysql.Open(dsn), &gc)
		if err != nil {
			panic(err)
		}
	case "postgres":
		dsn := sp[1]
		gormDB, err = gorm.Open(postgres.Open(dsn), &gc)
		if err != nil {
			panic(err)
		}
	}
	if gormDB == nil {
		panic(errors.New("unknown db type"))
	}

	db.db = gormDB
}
