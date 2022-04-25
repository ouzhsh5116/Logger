package main

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/client/v3"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	fmt.Println("connect to etcd success")

	defer cli.Close()

	// put
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//value := `[{"path":"/log/mysql.log","topic":"web_log"},{"path":"/log/redis.log","topic":"web_log"}]`
	//value := `[{"path":"./mysql.log","topic":"web_log"},{"path":"./redis.log","topic":"web_log"}]`
	value := `[{"path":"./mysql.log","topic":"web_log"}]`
	//cli.Delete(ctx,"/logagent/collect_config")
	_, err = cli.Put(ctx, "/logagent/192.168.1.101/collect_config", value)
	cancel()
	if err != nil {
		fmt.Printf("put to etcd failed, err:%v\n", err)
		return
	}
}
