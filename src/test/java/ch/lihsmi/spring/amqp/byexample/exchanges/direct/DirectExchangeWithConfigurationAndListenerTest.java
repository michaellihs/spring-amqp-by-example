package ch.lihsmi.spring.amqp.byexample.exchanges.direct;

import ch.lihsmi.spring.amqp.byexample.config.SimpleRabbitServerConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ch.lihsmi.spring.amqp.byexample.exchanges.direct.DirectExchangeWithConfigurationAndListenerTest.TestConfiguration.ROUTING_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DirectExchangeWithConfigurationAndListenerTest.TestConfiguration.class)
public class DirectExchangeWithConfigurationAndListenerTest {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private TestMessageListener listener;

    @Test
    public void directExchangeRoutesMessagesAsExpected() throws InterruptedException {
        byte[] messageBody = "direct message".getBytes();
        Message message = MessageBuilder.withBody(messageBody).build();

        template.convertAndSend(ROUTING_KEY, message);
        listener.latch.await(1, TimeUnit.SECONDS);

        assertThat(listener.receivedMessages.get(0).getBody(), is(messageBody));
    }

    @Configuration
    @Import(SimpleRabbitServerConfiguration.class)
    public static class TestConfiguration {

        private static final String DIRECT_EXCHANGE = "ExchangeTypesTests.DirectExchange";

        static final String ROUTING_KEY = "direct-exchange-routing-key";

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
            Queue queue = new Queue(ROUTING_KEY, true, false, false);
            rabbitAdmin.declareQueue(queue);
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY));
            return queue;
        }

        @Bean
        public TestMessageListener listener(DirectExchange exchange, Queue queue) {
            TestMessageListener listener = new TestMessageListener();
            SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
            container.setMessageListener(listener);
            container.setQueues(queue);
            container.start();
            return listener;
        }

        @Bean
        public RabbitTemplate rabbitTemplate() {
            RabbitTemplate template = new RabbitTemplate(connectionFactory);
            template.setExchange(DIRECT_EXCHANGE);
            return template;
        }

    }

    public static class TestMessageListener implements MessageListener {

        private final List<Message> receivedMessages = new ArrayList<>();

        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onMessage(Message message) {
            this.receivedMessages.add(message);
            latch.countDown();
        }

    }

}
