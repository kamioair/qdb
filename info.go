package qdb

import "encoding/json"

type Info map[string]any

// ToJson 将Info对象快速转为Json
func (i Info) ToJson() string {
	js, _ := json.Marshal(i)
	return string(js)
}

// Add 加入
func (i Info) Add(name string, value any) {
	i[name] = value
}

// Check 验证name是否存在
func (i Info) Check(name string) bool {
	_, ok := i[name]
	return ok
}
