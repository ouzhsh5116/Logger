package kafka

import (
	"fmt"
	"logtransfer/es"
	_ "sync"

	"github.com/Shopify/sarama"
)

//var wg sync.WaitGroup
// Init 初始化kafka消费者客户端
func Init(addr []string, topic string) error {
	// 创建消费者
	consumer, err := sarama.NewConsumer(addr, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return err
	}
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return err
	}

	fmt.Println("分区列表:", partitionList)
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return err
		}
		//defer pc.AsyncClose()
		// 异步从每个分区消费信息
		//wg.Add(1)
		go func(sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				//将kafka的数据发送到ES
				// logData := map[string]interface{}{
				// 	"data":string(msg.Value),
				// }
				// 将kafka的消息发到通道中，后台goroutine异步读取消息发送到es中
				logData := es.LogData{
					Topic: topic,
					Data:  string(msg.Value),
				}
				es.SendToESChan(&logData)
			}
			//wg.Done()
		}(pc)
	}
	//wg.Wait()
	return err
}
