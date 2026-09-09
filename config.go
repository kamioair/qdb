package qdb

// Setting 数据库配置
type Setting struct {
	Connect string `comment:"数据库连接串\n sqlite|./db/data.db&OFF\n sqlserver|用户名:密码@地址?database=数据库&encrypt=disable\n mysql|用户名:密码@tcp(127.0.0.1:3306)/数据库?charset=utf8mb4&parseTime=True&loc=Local"`
	Config  struct {
		OpenLog                bool
		SkipDefaultTransaction bool
		NoLowerCase            bool
	} `comment:"其他设置\n OpenLog：是否打开调试日志\n SkipDefaultTransaction：是否跳过默认事务\n NoLowerCase：是否不将结构体名和字段名转换为小写字母的形式"`
	sectionName string
	filePath    string
}

// NewDefaultSetting 默认配置
//
//	@param defaultConn 数据库连接串
//	         sqlite|./db/data.db&OFF
//	         sqlserver|用户名:密码@地址?database=数据库&encrypt=disable
//	         mysql|用户名:密码@tcp(127.0.0.1:3306)/数据库?charset=utf8mb4&parseTime=True&loc=Local
func NewDefaultSetting(defaultConn string) Setting {
	return NewSetting("./config.yaml", "DB", defaultConn, false, true, true)
}

// NewSetting 自定义配置
//
//	@param configFilePath: 配置文件路径
//	@param sectionName: 配置节点名称
//	@param defaultConn 数据库连接串
//	         sqlite|./db/data.db&OFF
//	         sqlserver|用户名:密码@地址?database=数据库&encrypt=disable
//	         mysql|用户名:密码@tcp(127.0.0.1:3306)/数据库?charset=utf8mb4&parseTime=True&loc=Local
//	@param openLog: 是否打开调试日志
//	@param skipDefaultTransaction: 是否跳过默认事务
//	@param noLowerCase: 是否不将结构体名和字段名转换为小写字母的形式
func NewSetting(configFilePath, sectionName string, defaultConn string, openLog, skipDefaultTransaction, noLowerCase bool) Setting {
	if defaultConn == "" {
		defaultConn = "sqlite|./db/data.db&OFF"
	}
	if configFilePath == "" {
		configFilePath = "./config.yaml"
	}
	if sectionName == "" {
		sectionName = "DB"
	}
	setting := Setting{
		Connect: defaultConn,
		Config: struct {
			OpenLog                bool
			SkipDefaultTransaction bool
			NoLowerCase            bool
		}{
			OpenLog:                openLog,
			SkipDefaultTransaction: skipDefaultTransaction,
			NoLowerCase:            noLowerCase,
		},
		sectionName: sectionName,
		filePath:    configFilePath,
	}
	return setting
}
