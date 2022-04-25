package main

import (
	"fmt"
	"logtransfer/config"
	"logtransfer/es"
	"logtransfer/kafka"

	"gopkg.in/ini.v1"
)

var (
	logTransferConf = new(config.LogTransferConf)
)

// 加载配置文件,并初始化Kafka ES
func init() {
	// 0.加载配置文件
	err := ini.MapTo(logTransferConf, "./config/config.ini")
	if err != nil {
		fmt.Printf("load ini file err:%v\n", err)
		return
	}
	fmt.Println("=======加载配置文件成功=======")

	// 1.初始化ES
	err = es.Init(logTransferConf.ESConf.Address, logTransferConf.ESConf.ChanSize, logTransferConf.ESConf.Nums)
	if err != nil {
		fmt.Printf("init ES fail,err :%v\n", err)
		return
	}
	fmt.Println("=======初始化ES成功=======")

	// 2.初始化kafka连接,创建分区消费者,每个分区的消费者分别取出数据通过sendToES发给ES
	err = kafka.Init([]string{logTransferConf.KafkaConf.Address}, logTransferConf.KafkaConf.Topic)
	if err != nil {
		fmt.Printf("init kafka fail,err :%v\n", err)
		return
	}
	fmt.Println("=======连接kafka成功=======")

}

// log transfer
// 功能:将kafka里的消息读取出来,发往ES
func main() {

	// 1.从Kafka取数据

	// 2.将数据发往ES

	select {}
}
