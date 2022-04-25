package conf

// 全局初始化配置
// AppConf里的结构体要与节的名对上加tag!!!
type AppConf struct {
	KafkaConf `ini:"kafka"`
	EtcdConf  `ini:"etcd"`
}

// kafka初始化配置结构体
type KafkaConf struct {
	Address     string `ini:"address"`
	ChanMaxSize int    `ini:"chan_max_size"`
}

type EtcdConf struct {
	Address string `ini:"address"`
	Key     string `ini:"collect_log_key"`
	Timeout int    `ini:"timeout"`
}

// taillog初始配置结构体
type TaillogConf struct {
	FilePath string `ini:"filepath"`
}
