package config

// 全局配项
// 需要加tag对应ini中的节
type LogTransferConf struct {
	KafkaConf `ini:"kafka"`
	ESConf    `ini:"es"`
}

// kafaka初始化配置项
type KafkaConf struct {
	Address string `ini:"address"`
	Topic   string `ini:"topic"`
}

// ES初始化配置项
type ESConf struct {
	Address  string `ini:"address"`
	ChanSize int    `ini:"chan_size"`
	Nums     int    `ini:"nums"`
}
