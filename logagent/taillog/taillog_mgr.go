package taillog

import (
	"fmt"
	"logagent/etcd"
	"time"
)

var tskMgr *tailLogMgr

// tailTask 管理者
type tailLogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &tailLogMgr{
		logEntry:    logEntryConf, // 把当前的日志收集项配置信息保存起来
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), //无缓冲区通道
	}

	for _, logEntry := range logEntryConf {
		//conf: *etcd.LogEntry
		//	conf:一条日志收集配置项的结构体指针
		//	为每一条日志收集配置项创建一个taillogObj,通过taillog_mng管理这些taillogObj
		tailTaskObj := NewTailTask(logEntry.Path, logEntry.Topic)

		//以路劲+topic为键
		mk := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		// 初始化的每条tailtask需要记录存下来,以路劲+topic为键
		tskMgr.tskMap[mk] = tailTaskObj

	}
	// 开启goroutine监听获取新配置项！！！
	go tskMgr.run()
}

// 监听自己的newConfChan通道，有新配置过来就做处理 1.配置新增 2.配置删除 3.配置更新
func (t *tailLogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			for _, conf := range newConf {
				mk := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				_, ok := t.tskMap[mk]
				if ok {
					//初始化就有了这个路径，不需要操作
					continue
				} else {
					//新增的,再创建新的tailTask做处理
					tailObj := NewTailTask(conf.Path, conf.Topic)
					//以路劲+topic为键
					// 初始化的每条tailtask需要记录存下来,以路劲+topic为键
					// 读取日志文件发到chan中

					t.tskMap[mk] = tailObj
				}
			}
			// 找出原来t.tskMap中有的配置，但是newCof中没有的配置，进行删除
			for _, c1 := range t.logEntry {
				isDelete := true
				for _, c2 := range newConf {
					if c2.Path == c1.Path && c2.Topic == c1.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					// 新的配置项里面没有该配置项,c1的旧配置项有该配置项，删除旧的配置项和停止旧配置项任务
					mk := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.tskMap[mk].cancelFunc()
				}
			}
			//1.配置新增

			//2.配置删除

			// 3.配置更新

			fmt.Println("新的配置来了:", newConf)
		default:
			time.Sleep(time.Millisecond * 500)
		}
	}

}

// 向外暴露一个 tailLogMgr的newConfChan
func GetNewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
