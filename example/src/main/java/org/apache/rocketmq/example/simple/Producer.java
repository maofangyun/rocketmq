/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        // 相同的ProducerGroup不同的producer是否会在producerTable中只存储一个？会报错MQClientException
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setInstanceName("test");
        producer.start();

        MessageSend messageSend = new MessageSend();
        for (int i = 0; i < 3; i++) {
            try {
                Message msg = new Message("TopicTest1",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                messageSend.message(producer,msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //producer.shutdown();
    }
}

class MessageSend {
    public void message(DefaultMQProducer producer,Message msg) throws Exception {
        //1、同步
        sync(producer,msg);
        //2、异步
        //async(producer,msg);
        //3、单项发送
        //oneWay(producer,msg);
    }
    /**
     * 1、同步发送消息
     */
    private  void sync(DefaultMQProducer producer,Message msg) throws Exception {
        //同步发送消息
        SendResult sendResult = producer.send(msg);
        System.out.printf("%s%n", sendResult);
    }
    /**
     * 2、异步发送消息
     */
    private  void async(DefaultMQProducer producer,Message msg) throws Exception {
        //异步发送消息
        producer.send(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("Product-异步发送-输出信息={"+sendResult+"}");
            }
            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                //补偿机制，根据业务情况进行使用，看是否进行重试
            }
        });
    }
    /**
     * 3、单项发送消息
     */
    private  void oneWay(DefaultMQProducer producer,Message msg) throws Exception {
        //同步发送消息
        producer.sendOneway(msg);
    }
}
