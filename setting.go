package qdb

import (
	"encoding/json"
	"os"
)

type setting struct {
	Connect string `comment:"数据库连接串\n sqlite|./db/data.db&OFF  OFF=(DELETE/MEMORY/WAL/OFF)\n sqlserver|用户名:密码@地址?database=数据库&encrypt=disable\n mysql|用户名:密码@tcp(127.0.0.1:3306)/数据库?charset=utf8mb4&parseTime=True&loc=Local"`
	Config  struct {
		OpenLog                bool
		SkipDefaultTransaction bool
		NoLowerCase            bool
	} `comment:"其他设置\n OpenLog：是否打开调试日志\n SkipDefaultTransaction：是否跳过默认事务\n NoLowerCase：是否不将结构体名和字段名转换为小写字母的形式"`
	filePath string
}

func initBaseConfig() *setting {
	config := &setting{
		filePath: "./config.yaml",
		Connect:  "sqlite|./db/data.db&OFF",
		Config: struct {
			OpenLog                bool
			SkipDefaultTransaction bool
			NoLowerCase            bool
		}{
			OpenLog:                false,
			SkipDefaultTransaction: true,
			NoLowerCase:            true,
		},
	}

	if len(os.Args) > 1 {
		args := map[string]string{}
		err := json.Unmarshal([]byte(os.Args[1]), &args)
		if err != nil {
			panic(err)
		}
		// 自定义配置文件路径
		if val, ok := args["ConfigPath"]; ok {
			config.filePath = val
		}
	}

	return config
}
