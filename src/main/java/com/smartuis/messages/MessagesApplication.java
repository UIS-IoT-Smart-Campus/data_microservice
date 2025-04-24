package com.smartuis.messages;

import com.smartuis.messages.service.DeviceMessageMqttService;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

@SpringBootApplication
public class MessagesApplication {

    private String broker_ip = System.getenv("BROKER_IP") != null ? System.getenv("BROKER_IP") : "localhost";
    private String brokerIp = "tcp://"+broker_ip+":1883";
    


//    private String brokerIp = "tcp://localhost:1883";
    @Value("${BROKER_CLIENT_ID:data_microservice}")
    private String clientId;
    @Value("${BROKER_TOPIC:smartCampus/#}")
    private String topic;
    @Value("${BROKER_USERNAME:admin}")
    private String username;
    @Value("${BROKER_PASSWORD:public}")
    private String password;

    @Autowired
    private DeviceMessageMqttService mqttService;

    public static void main(String[] args) {
        SpringApplication.run(MessagesApplication.class, args);
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel mqttOutputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageProducer inbound() {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(brokerIp, clientId, topic);
        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(2);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler handler() {
        return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                try {
                    String payload = (String) message.getPayload();

                    // Buscar el campo "topic" en el payload
                    String topicKey = "\"topic\":\"";
                    int startIndex = payload.indexOf(topicKey) + topicKey.length();
                    int endIndex = payload.indexOf("\"", startIndex);

                    if (endIndex != -1) {
                        String destinationTopic = payload.substring(startIndex, endIndex);

                        // Guardar el mensaje en la base de datos
                        mqttService.saveMqttDeviceMessage(message);

                        // Enviar el mensaje nuevamente al tópico extraído
                        if (!destinationTopic.isEmpty()) {
                            mqttOutputChannel().send(
                                    org.springframework.integration.support.MessageBuilder
                                            .withPayload(payload)
                                            .setHeader(MqttHeaders.TOPIC, destinationTopic)
                                            .build()
                            );
                        }
                    } else {
                        throw new MessagingException("Topic not found in the payload");
                    }
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        };
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttOutputChannel")
    public MessageHandler mqttOutbound() {
        MqttPahoMessageHandler messageHandler =
                new MqttPahoMessageHandler(clientId + "_outbound", mqttClientFactory());
        messageHandler.setAsync(true);
        return messageHandler;
    }

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setServerURIs(new String[]{brokerIp});
        System.out.println(brokerIp);
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        factory.setConnectionOptions(options);
        return factory;
    }
}
