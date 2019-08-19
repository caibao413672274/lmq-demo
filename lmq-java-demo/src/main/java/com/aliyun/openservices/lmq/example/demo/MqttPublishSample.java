package com.aliyun.openservices.lmq.example.demo;
import com.aliyun.openservices.lmq.example.util.SHA256Util;
import com.aliyun.openservices.lmq.example.util.Tools;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.eclipse.paho.client.mqttv3.MqttConnectOptions.MQTT_VERSION_3_1_1;

public class MqttPublishSample {

    public static void main(String[] args) throws InterruptedException, NoSuchAlgorithmException, InvalidKeyException {
        final String parentTopic = "Demo/TestTopic";
        final int qosLevel = 2;
        String content      = "Message from MqttPublishSample";
//        int qos             = 2;
        String broker       = "tcp://10.1.5.83:1883";
        String clientId     = "Publish001";
        MemoryPersistence persistence = new MemoryPersistence();
//        final ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
//                new LinkedBlockingQueue<Runnable>());
        try {
            final     MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            /**
             * 客户端设置好发送超时时间，防止无限阻塞
             */
            sampleClient.setTimeToWait(5000);
            sampleClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    /**
                     * 客户端连接成功后就需要尽快订阅需要的 topic
                     */
                    System.out.println("connect success");
//                    executorService.submit(new Runnable() {
//                        @Override
//                        public void run() {
//                            try {
//                                final String topicFilter[] = {parentTopic};
//                                final int[] qos = {qosLevel};
//                                sampleClient.subscribe(topicFilter, qos);
//                            } catch (MqttException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    });
                }

                @Override
                public void connectionLost(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    /**
                     * 消费消息的回调接口，需要确保该接口不抛异常，该接口运行返回即代表消息消费成功。
                     * 消费消息需要保证在规定时间内完成，如果消费耗时超过服务端约定的超时时间，对于可靠传输的模式，服务端可能会重试推送，业务需要做好幂等去重处理。超时时间约定参考限制
                     * https://help.aliyun.com/document_detail/63620.html?spm=a2c4g.11186623.6.546.229f1f6ago55Fj
                     */
                    System.out.println(
                            "receive msg from topic " + s + " , body is " + new String(mqttMessage.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    System.out.println("send msg succeed topic is : " + iMqttDeliveryToken.getTopics()[0]);
                }
            });


            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions.setUserName("test0001" );

                        mqttConnectOptions.setPassword("12345678".toCharArray());
            mqttConnectOptions.setCleanSession(true);
            mqttConnectOptions.setKeepAliveInterval(90);
            mqttConnectOptions.setAutomaticReconnect(true);
            mqttConnectOptions.setMqttVersion(MQTT_VERSION_3_1_1);
            mqttConnectOptions.setConnectionTimeout(5000);

            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(mqttConnectOptions);
//            System.out.println("Connected");

            final String p2pSendTopic = parentTopic + "/p2p/" + clientId;


            int num=0;
            while (num<100000) {
                System.out.println("Publishing message: " + content);
//                MqttMessage message = new MqttMessage("hello mq4Iot p2p msg".getBytes());
//                message.setQos(qosLevel);
//                sampleClient.publish(p2pSendTopic, message);
                MqttMessage message = new MqttMessage((content+num).getBytes());
                message.setQos(qosLevel);
                sampleClient.publish(parentTopic, message);
//                System.out.println("Message published");
                num++;
            Thread.sleep(5000);
            }
            sampleClient.disconnect();
            System.out.println("Disconnected");
            System.exit(0);
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }
    }
}