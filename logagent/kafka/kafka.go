package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// kafka 将收集的日志发送给kafka

type logData struct {
	topic string
	data  string
}

var (
	// 全局连接kafka的生产者客户端
	client      sarama.SyncProducer
	logDataChan chan *logData
)

// Init 初始化kafka客户端
func Init(addr []string, maxSize int) (err error) {
	// 新建配置实例
	config := sarama.NewConfig()
	// 修改配置实例的参数
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出⼀个 partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	// 按指定配置连接kafka,获得客户端实例
	client, err = sarama.NewSyncProducer(addr, config)
	if err != nil {
		return
	}

	// 初始化logDataChan
	logDataChan = make(chan *logData, maxSize)

	// 初始化kafka客户端的时候在后台开启goroutine,一直等待读取logDataChan的数据发往kafka***
	go sendToKafka()
	// defer client.Close()
	return
}

// 对外暴露的写入内部chan中的函数
func SendToChan(topic, data string) {
	logDataChan <- &logData{
		topic: topic,
		data:  data,
	}
}

//	真正按主题发送数据data给kafka
func sendToKafka() {
	for {
		select {
		case msg := <-logDataChan:
			// 构造⼀个生产者消息对象
			pdMsg := &sarama.ProducerMessage{}
			// 发送消息的主题
			pdMsg.Topic = msg.topic
			// 消息值编码
			pdMsg.Value = sarama.StringEncoder(msg.data)

			// 客户端向kafka发送消息
			pid, offset, err := client.SendMessage(pdMsg)
			if err != nil {
				fmt.Printf("kafka client.SendMessage failed err:%v\n", err)
				return
			}

			fmt.Printf("pid:%v offset:%v data:%v\n", pid, offset, msg.data)
			fmt.Println("发送到Kafka成功!")
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}

}
