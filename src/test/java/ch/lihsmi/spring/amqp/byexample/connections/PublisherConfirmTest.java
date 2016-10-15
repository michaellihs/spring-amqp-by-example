package ch.lihsmi.spring.amqp.byexample.connections;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.ConfirmCallback;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = PublisherConfirmTest.TestConfiguration.class)
public class PublisherConfirmTest {

    private static final String DIRECT_EXCHANGE = "ConnectionsTests.PublisherConfirm";

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private RabbitAdmin admin;

    @Autowired
    private MessageListenerTestImplementation validListener;

    @Autowired
    private ConfirmCallbackTestImplementation testConfirmCallback;

    @After
    public void resetBroker() throws InterruptedException {
        admin.deleteQueue("validListener");
        admin.deleteExchange(DIRECT_EXCHANGE);
    }

    @Test
    public void sendMessageToValidMessageListenerIsConfirmedAsExpected() throws InterruptedException {
        String correlationId = "corr-data-test-1";
        Message message = MessageBuilder.withBody("valid message".getBytes())
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .build();
        CountDownLatch confirmationLatch = testConfirmCallback.expect(correlationId);

        template.convertAndSend("ConnectionsTests.PublisherConfirm", "validListener", message, new CorrelationData(correlationId));

        assertTrue(validListener.latch.await(1, TimeUnit.SECONDS));
        assertTrue(confirmationLatch.await(1, TimeUnit.SECONDS));

        assertThat(validListener.receivedMessages.size(), is(1));
        assertThat(testConfirmCallback.confirmations.get(correlationId), is(true));
    }

    @Configuration
    public static class TestConfiguration {

        @Bean
        public ConnectionFactory connectionFactory() {
            CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
            connectionFactory.setPublisherConfirms(true);
            return connectionFactory;
        }

        @Bean
        public ConfirmCallback confirmCallback() {
            return new ConfirmCallbackTestImplementation();
        }

        @Bean
        public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, ConfirmCallback confirmCallback) {
            RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
            rabbitTemplate.setConfirmCallback(confirmCallback);
            rabbitTemplate.setExchange(DIRECT_EXCHANGE);
            return rabbitTemplate;
        }

        @Bean
        public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
            return new RabbitAdmin(connectionFactory);
        }

        @Bean
        public DirectExchange directExchange(RabbitAdmin rabbitAdmin) {
            DirectExchange exchange = new DirectExchange(DIRECT_EXCHANGE);
            rabbitAdmin.declareExchange(exchange);
            return exchange;
        }

        @Bean
        public MessageListenerTestImplementation validListener(RabbitAdmin rabbitAdmin, DirectExchange exchange, ConnectionFactory connectionFactory) {
            Queue queue = queue(rabbitAdmin, exchange, "validListener");
            MessageListenerTestImplementation listener = new MessageListenerTestImplementation();
            SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
            container.setMessageListener(listener);
            container.setQueues(queue);
            container.start();
            return listener;
        }

        private Queue queue(RabbitAdmin rabbitAdmin, DirectExchange exchange, String routingKey) {
            Queue queue = new Queue(routingKey, true, false, true);
            rabbitAdmin.declareQueue(queue);
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(routingKey));
            return queue;
        }

    }

    public  static class MessageListenerTestImplementation implements MessageListener {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final List<Message> receivedMessages = new ArrayList<>();

        @Override
        public void onMessage(Message message) {
            receivedMessages.add(message);
            latch.countDown();
        }

    }

    public static class ConfirmCallbackTestImplementation implements ConfirmCallback {

        private volatile Map<String, Boolean> confirmations = new HashMap<>();

        private volatile HashMap<String, CountDownLatch> expectationLatches = new HashMap<>();

        @Override
        public void confirm(CorrelationData correlationData, boolean success, String s) {
            confirmations.put(correlationData.getId(), success);
            expectationLatches.get(correlationData.getId()).countDown();
        }

        public CountDownLatch expect(String correlationId) {
            CountDownLatch latch = new CountDownLatch(1);
            this.expectationLatches.put(correlationId, latch);
            return latch;
        }

    }

}
