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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DirectExchangeWithMultipleListenersTest.TestConfiguration.class)
public class DirectExchangeWithMultipleListenersTest {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private DirectExchange exchange;

    @Test
    public void directExchangeRoutesMessagesAsExpected() throws InterruptedException {
        TestMessageListener listener1 = createAndBindListener("routing-key-1");
        TestMessageListener listener2 = createAndBindListener("routing-key-2", 2);

        rabbitTemplate.convertAndSend("routing-key-1", buildMessage("message body 1"));
        rabbitTemplate.convertAndSend("routing-key-2", buildMessage("message body 2"));
        rabbitTemplate.convertAndSend("routing-key-2", buildMessage("message body 3"));

        listener1.latch.await(1, TimeUnit.SECONDS);
        listener2.latch.await(2, TimeUnit.SECONDS);

        assertThat(listener1.receivedMessages.size(), is(1));
        assertThat(listener1.receivedMessages.get(0).getBody(), is("message body 1".getBytes()));
        assertThat(listener2.receivedMessages.size(), is(2));
        assertThat(listener2.receivedMessages.get(0).getBody(), is("message body 2".getBytes()));
        assertThat(listener2.receivedMessages.get(1).getBody(), is("message body 3".getBytes()));
    }

    private Message buildMessage(String messageBody) {
        return MessageBuilder.withBody(messageBody.getBytes()).build();
    }

    private TestMessageListener createAndBindListener(String routingKey) {
        return createAndBindListener(routingKey, 1);
    }

    private TestMessageListener createAndBindListener(String routingKey, int expectedMessages) {
        TestMessageListener listener = new TestMessageListener(expectedMessages);
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setMessageListener(listener);
        container.setQueues(declareQueue(routingKey));
        container.start();
        return listener;
    }

    private Queue declareQueue(String routingKey) {
        Queue queue = new Queue(routingKey, true, false, false);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(routingKey));
        return queue;
    }

    @Configuration
    @Import(SimpleRabbitServerConfiguration.class)
    public static class TestConfiguration {

        private static final String DIRECT_EXCHANGE = "ExchangeTypesTests.DirectExchange";

        @Bean
        public DirectExchange directExchange(RabbitAdmin rabbitAdmin) {
            DirectExchange exchange = new DirectExchange(DIRECT_EXCHANGE);
            rabbitAdmin.declareExchange(exchange);
            return exchange;
        }

        @Bean
        public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RabbitTemplate template = new RabbitTemplate(connectionFactory);
            template.setExchange(DIRECT_EXCHANGE);
            return template;
        }

    }

    public static class TestMessageListener implements MessageListener {

        private final List<Message> receivedMessages = new ArrayList<>();

        private final CountDownLatch latch;

        public TestMessageListener(int expectedMessages) {
            latch = new CountDownLatch(expectedMessages);
        }

        @Override
        public void onMessage(Message message) {
            this.receivedMessages.add(message);
        }

    }

}
