package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"strings"
	"time"
)

type LogData struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

var (
	client *elastic.Client
	ch     chan *LogData
)

// 初始化ES,准备接受kafka消费者发来的数据
func Init(addr string, chanSize int, nums int) (err error) {
	var pre = "http://"
	if !strings.HasPrefix(addr, pre) {
		addr = pre + addr
	}
	client, err = elastic.NewClient(elastic.SetURL(addr))
	if err != nil {
		// Handle error
		panic(err)
	}
	ch = make(chan *LogData, chanSize)
	for i := 0; i < nums; i++ {

		go sendToES()
	}
	return err
}

// 发送数据到ES
func SendToESChan(msg *LogData) {
	ch <- msg
	// 向es发数据

}

func sendToES() {
	//fmt.Printf(index ,data
	for {
		select {
		case msg := <-ch:
			put1, err := client.Index().Index(msg.Topic).BodyJson(msg).Do(context.Background())
			if err != nil {
				// Handle error
				fmt.Println(err)
				continue
			}
			fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
		default:
			time.Sleep(time.Second)
		}
	}

}
