package main

import (
	"fmt"
	"logagent/conf"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/taillog"
	"logagent/utils"
	"sync"
	"time"

	"gopkg.in/ini.v1"
)

var (
	appConf = new(conf.AppConf) //全局初始化配置,要new初始化否则报错!!
	wg      sync.WaitGroup
)

func init() {
	// 0.加载初始化配置文件
	err := ini.MapTo(appConf, "./conf/config.ini")
	if err != nil {
		fmt.Printf("load ini file err:%v\n", err)
		return
	}
	fmt.Println("=======加载配置文件成功=======")

	// 1.初始化etcd客户端
	err = etcd.Init(appConf.EtcdConf.Address, time.Duration(appConf.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	fmt.Println("=======开启etcd客户端成功=======")

	// 2.初始化kafka连接
	err = kafka.Init([]string{appConf.KafkaConf.Address}, appConf.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Printf("init kafka fail,err :%v\n", err)
		return
	}
	fmt.Println("=======连接kafka成功=======")
}

// logagent 程序入口
func main() {

	// 添加配置项
	// value := `[{"path":"/tmp/nginx.log","topic":"web_log"},{"path":"/tmp/redis.log","topic":"redis_log"},{"path":"/tmp/mysql.log","topic":"mysql_log"}]`
	// err = etcd.PutConf("/logagent/collect_config", value)
	// if err != nil {
	// 	return
	// }

	//为了实现每个logagent都按自己的ip拉取自己的配置项信息
	ip, err := utils.GetOutboundIP()
	if err != nil {
		panic(err)
	}
	// 替换配置文件中%s为ip
	etcdConfKey := fmt.Sprintf(appConf.EtcdConf.Key, ip)

	// 2.1从etcd中获取日志收集配置项的信息
	logEntryConf, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		return
	}
	for index, value := range logEntryConf {
		fmt.Printf("index :%v,path: %#v,topic:%#v\n", index, value.Path, value.Topic)
	}
	// example  index :0,path: "/tmp/nginx.log",topic:"web_log"
	// 			index :1,path: "/tmp/redis.log",topic:"redis_log"

	// 3.收集日志发往kakfa
	// 3.1循环每个日志收集项，创建tailobj
	// 3.2将tailobj收集的日志发往kafka
	//后台多个goroutine处理日志数据
	taillog.Init(logEntryConf)

	// 先初始化再获取newconfchan！！！
	// 获取taillog中的配置更新通道
	newConfChan := taillog.GetNewConfChan()

	// 2.2开启一个后台携程哨兵监视日志收集配置项的变化情况（有变化通知logAgent实现配置热加载）
	wg.Add(1)
	// 将变更的新配置项消息发给newConfChan
	go etcd.WatchConf(etcdConfKey, newConfChan)

	// // 2.打开日志文件准备收集日志
	// err = taillog.Init(appConf.TaillogConf.FilePath)
	// if err != nil {
	// 	fmt.Printf("init taillog fail,err :%v\n", err)
	// }
	// fmt.Println("=======初始化tailog成功=======")

	// // 程序执行
	// run()
	wg.Wait()
}
