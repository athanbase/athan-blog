---
title: "kafka-go 使用"
date: 2022-11-13T12:17:44+08:00
draft: false
---
- ## 创建topic  
-  
  ``` go
	      conn, err := kafka.Dial("tcp", "localhost:9092")
	  	if err != nil {
	  		panic(err.Error())
	  	}
	  	defer conn.Close()
	  
	  	controller, err := conn.Controller()
	  	if err != nil {
	  		panic(err.Error())
	  	}
	  	var controllerConn *kafka.Conn
	  	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	  	if err != nil {
	  		panic(err.Error())
	  	}
	  	defer controllerConn.Close()
	  
	      topic := "my-topic"
	  	partitionCount := 3
	  	replicationFactor := 3
	  
	  	topicConfigs := []kafka.TopicConfig{
	  		{
	  			Topic:             topic,
	  			NumPartitions:     3,
	  			ReplicationFactor: 1,
	  		},
	  	}
	  
	  	err = controllerConn.CreateTopics(topicConfigs...)
	  	if err != nil {
	  		panic(err.Error())
	  	}
	  	fmt.Printf("topic: %s create success\n", topic)
  ```
-  
- ## 写入消息  
-  
  ```
	      topic := "my-topic"
	      w := &kafka.Writer{
	  		Addr:         kafka.TCP("localhost:9092"),
	  		Topic:        topic,
	  		Balancer:     &kafka.RoundRobin{},
	  		Async:        true, // 异步写入
	  		BatchTimeout: time.Millisecond * 100,
	  		BatchSize:    10,
	  		RequiredAcks: kafka.RequireOne, // leader写入算成功
	          Compression: kafka.Lz4, // 压缩算法
	  	}
	      
	      v := map[string]any{
	  		"foo": "bar",
	          "v": 123
	  	}
	  	data, _ := json.Marshal(v)
	  
	  	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	  	_ := w.WriteMessages(ctx,
	  		kafka.Message{Value: data},
	  	) // 异步写入不返回错误
	  	cancel()
	      
	      err = w.Close()
	  	if err != nil {
	  		panic(err)
	  	}
  ```
-  
- ## 消费消息  
-  
  ``` go
	  	reader := kafka.NewReader(kafka.ReaderConfig{
	  		Brokers:     []string{"localhost:9092"},
	  		Topic:       "my-topic",
	  		GroupID:     "group-id",
	  		StartOffset: kafka.FirstOffset,
	  		Dialer: &kafka.Dialer{
	  			Timeout:   time.Second * 10,
	  			DualStack: true,
	  			ClientID:  "consumer1",
	  		},
	  		MaxWait:        500 * time.Millisecond,
	  		CommitInterval: time.Second,
	  	})
	  
	  	for {
	  		msg, err := reader.FetchMessage(context.Background())
	  		if err != nil {
	  			panic(err)
	  		}
	  
	  		fmt.Printf("partition: %d, offset: %d, msg: %s", msg.Partition, msg.Offset, string(msg.Value))
	  		// commit msg
	  		err = reader.CommitMessages(context.Background(), msg)
	  		if err != nil {
	  			panic(err)
	  		}
	  	}
  ```