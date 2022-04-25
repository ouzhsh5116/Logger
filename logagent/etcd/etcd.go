package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"time"
)

// etcd watch

var cli *clientv3.Client

// 收集日志的配置实体,以json的格式作为value传入etcd中
type LogEntry struct {
	Path  string `json:"path"`  //读日志的文件路径
	Topic string `json:"topic"` //写入kafka的topic
}

// 初始化etcd
func Init(address string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: timeout,
	})
	if err != nil {
		// handle error!
		return
	}
	return
}

// 向etcd添加配置项
func PutConf(key string, json string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = cli.Put(ctx, key, json)
	cancel()
	if err != nil {
		fmt.Printf("put to etcd failed, err:%v\n", err)
		return
	}
	return
}

// 根据key从ETCD中获取配置项,解析配置的json格式,以结构体类型的切片存储
func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	// get
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		// example: /xxx:[{"path":"/tmp/nginx.log","topic":"web_log"},{"path":"/tmp/redis.log","topic":"redis_log"}]
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Printf("unmarshal etcd value failed,err:%v\n", err)
			return
		}
	}
	return
}

//defer cli.Close()

// watch
// 派一个哨兵后台 一直监视着这个key的变化（新增、修改、删除），新变化配置写入newConfCh
func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	ch := cli.Watch(context.Background(), key)
	// 从通道尝试取值(监视的信息)
	for wresp := range ch {
		for _, evt := range wresp.Events {
			fmt.Printf("Type:%v key:%v value:%v\n", evt.Type, string(evt.Kv.Key), string(evt.Kv.Value))
			// 配置发生修改时通知tailtask执行不同的配置更新
			var newConf []*LogEntry
			//判断操作类型
			if evt.Type != clientv3.EventTypeDelete {
				// 手动传递一个空的配置项
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Printf("json.Unmarshal(evt.Kv.Value,&newConf) faile:%v\n", err)
					continue
				}
			}
			newConfCh <- newConf
		}
	}
}
