package taillog

import (
	"context"
	"fmt"
	"logagent/kafka"
	"time"

	"github.com/hpcloud/tail"
)

// tailLog 从日志文件中收集日志

// 处理日志收集的tail任务
type TailTask struct {
	path     string
	topic    string
	instance *tail.Tail
	// 采用context记录上下文，实现删除配置项退出t.run()
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// 构造函数初始化tailTask实例
func NewTailTask(path, topic string) (tailTaskObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailTaskObj = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailTaskObj.init()
	return
}

func (t *TailTask) init() {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件的哪个地方开始读
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,
	}
	var err error
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
	}

	go t.run() // 直接去采集日志发送到kafka
}

// tailTask读取日志发送到chan,再读chan发送到kafka
func (t *TailTask) run() {
	for {
		select {
		// 当收到ctx.Cancle函数时收到停止信号，该task退出
		case <-t.ctx.Done():
			fmt.Printf("tail task :%s_%s exit....\n", t.path, t.topic)
			return
		case line := <-t.instance.Lines:
			//方案一 函数掉函数问题可以转化为通过通道发送信息，后台goroutine任务在通道取数据，实现异步，提高执行效率。
			//kafka.SendToKafka(t.topic, line.Text)

			//优化方案二 发日志数据到通道中
			kafka.SendToChan(t.topic, line.Text)
			//kafka包中定义方法去读日志发送到kafka中
		default:
			time.Sleep(time.Second)
		}
	}
}
