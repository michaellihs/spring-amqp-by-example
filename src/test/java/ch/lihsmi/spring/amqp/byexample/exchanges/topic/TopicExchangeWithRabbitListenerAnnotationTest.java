package ch.lihsmi.spring.amqp.byexample.exchanges.topic;

import ch.lihsmi.spring.amqp.byexample.config.SimpleRabbitServerConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TopicExchangeWithRabbitListenerAnnotationTest.TestConfiguration.class)
public class TopicExchangeWithRabbitListenerAnnotationTest {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private MessageProcessor messageProcessor;

    @Test
    public void messagesAreProcessedAsExpected() throws InterruptedException {
        template.convertAndSend("logs.1", "Log message 1");
        template.convertAndSend("logs.2", "Log message 2");
        template.convertAndSend("logs.3", "Log message 3");
        template.convertAndSend("messages.1", "message 1");
        template.convertAndSend("messages.2", "message 2");

        messageProcessor.latch.await(1, TimeUnit.SECONDS);

        assertThat(messageProcessor.receivedLogMessages.size(), is(3));
        assertThat(messageProcessor.receivedMessages.size(), is(2));
    }

    @Configuration
    @EnableRabbit
    @Import(SimpleRabbitServerConfiguration.class)
    public static class TestConfiguration {

        private static final String TOPIC_EXCHANGE = "ExchangeTypesTests.TopicExchange";

        @Bean
        public TopicExchange topicExchange(RabbitAdmin rabbitAdmin) {
            TopicExchange exchange = new TopicExchange(TOPIC_EXCHANGE);
            rabbitAdmin.declareExchange(exchange);
            return exchange;
        }

        @Bean
        public Queue logsQueue(RabbitAdmin rabbitAdmin, TopicExchange exchange) {
            Queue queue = new Queue("logs-queue", true, false, false);
            rabbitAdmin.declareQueue(queue);
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("logs.*"));
            return queue;
        }

        @Bean
        public Queue messageQueue(RabbitAdmin rabbitAdmin, TopicExchange exchange) {
            Queue queue = new Queue("messages-queue", true, false, false);
            rabbitAdmin.declareQueue(queue);
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("messages.*"));
            return queue;
        }

        @Bean
        public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RabbitTemplate template = new RabbitTemplate(connectionFactory);
            template.setExchange(TOPIC_EXCHANGE);
            return template;
        }

        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
            SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);
            return factory;
        }

        @Bean
        public MessageProcessor messageProcessor() {
            return new MessageProcessor(5);
        }

    }

    @Component
    public static class MessageProcessor {

        private final List<String> receivedLogMessages = new ArrayList<>();
        private final List<String> receivedMessages = new ArrayList<>();

        private final CountDownLatch latch;

        MessageProcessor(int expectedMessages) {
            latch = new CountDownLatch(expectedMessages);
        }

        @RabbitListener(queues = "logs-queue")
        public void processLogMessage(String message) {
            this.receivedLogMessages.add(message);
            latch.countDown();
        }

        @RabbitListener(queues = "messages-queue")
        public void processMessage(String message) {
            this.receivedMessages.add(message);
            latch.countDown();
        }

    }

}
