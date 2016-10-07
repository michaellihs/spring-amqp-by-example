package ch.lihsmi.spring.amqp.byexample.exchanges.direct;

import ch.lihsmi.spring.amqp.byexample.config.SimpleRabbitServerConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.*;
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
@SpringBootTest(classes = DirectExchangeWithRabbitListenerAnnotationTest.TestConfiguration.class)
public class DirectExchangeWithRabbitListenerAnnotationTest {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private AnnotationBasedMessageListener listener;

    @Test
    public void directExchangeRoutesMessagesAsExpected() throws InterruptedException {
        byte[] messageBody = "direct message".getBytes();
        Message message = MessageBuilder.withBody(messageBody).build();

        template.convertAndSend(TestConfiguration.ROUTING_KEY, message);
        listener.latch.await(1, TimeUnit.SECONDS);

        assertThat(listener.receivedMessages.size(), is(1));
        assertThat(listener.receivedMessages.get(0).getBody(), is(messageBody));
    }

    @Configuration
    @EnableRabbit
    @Import(SimpleRabbitServerConfiguration.class)
    public static class TestConfiguration {

        private static final String DIRECT_EXCHANGE = "ExchangeTypesTests.DirectExchange";

        static final String QUEUE_NAME = "annotation-based-queue";

        static final String ROUTING_KEY = "routing-key";

        @Autowired
        private RabbitAdmin rabbitAdmin;

        @Autowired
        private ConnectionFactory connectionFactory;

        @Bean
        public DirectExchange directExchange() {
            DirectExchange exchange = new DirectExchange(DIRECT_EXCHANGE);
            rabbitAdmin.declareExchange(exchange);
            return exchange;
        }

        @Bean
        public Queue queue(DirectExchange exchange) {
            Queue queue = new Queue(QUEUE_NAME, true, false, false);
            rabbitAdmin.declareQueue(queue);
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY));
            return queue;
        }

        @Bean
        public RabbitTemplate rabbitTemplate() {
            RabbitTemplate template = new RabbitTemplate(connectionFactory);
            template.setExchange(DIRECT_EXCHANGE);
            return template;
        }

        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
            SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);
            return factory;
        }

        @Bean
        public AnnotationBasedMessageListener annotationBasedMessageListener() {
            return new AnnotationBasedMessageListener();
        }

    }

    @Component
    public static class AnnotationBasedMessageListener {

        private final List<Message> receivedMessages = new ArrayList<>();

        private final CountDownLatch latch = new CountDownLatch(1);

        @RabbitListener(queues = "annotation-based-queue")
        public void onMessage(Message message) {
            this.receivedMessages.add(message);
            latch.countDown();
        }

    }

}
