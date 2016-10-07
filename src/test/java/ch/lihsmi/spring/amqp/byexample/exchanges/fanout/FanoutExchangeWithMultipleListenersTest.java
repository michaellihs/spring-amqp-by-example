package ch.lihsmi.spring.amqp.byexample.exchanges.fanout;

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
@SpringBootTest(classes = FanoutExchangeWithMultipleListenersTest.TestConfiguration.class)
public class FanoutExchangeWithMultipleListenersTest {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private FanoutExchange fanoutExchange;

    @Test
    public void fanoutExchangeDistributesMessagesAsExpected() throws InterruptedException {
        byte[] messageBody = "fanout message".getBytes();
        Message message = MessageBuilder.withBody(messageBody).build();

        TestMessageListener listener1 = createAndBindMessageListener("fanout-declareQueue-1");
        TestMessageListener listener2 = createAndBindMessageListener("fanout-declareQueue-2");
        TestMessageListener listener3 = createAndBindMessageListener("fanout-declareQueue-3");

        template.convertAndSend(message);

        listener1.latch.await(1, TimeUnit.SECONDS);
        listener2.latch.await(1, TimeUnit.SECONDS);
        listener3.latch.await(1, TimeUnit.SECONDS);

        assertThat(listener1.receivedMessages.size(), is(1));
        assertThat(listener2.receivedMessages.size(), is(1));
        assertThat(listener3.receivedMessages.size(), is(1));

        assertThat(listener1.receivedMessages.get(0).getBody(), is(messageBody));
        assertThat(listener2.receivedMessages.get(0).getBody(), is(messageBody));
        assertThat(listener3.receivedMessages.get(0).getBody(), is(messageBody));
    }

    @Test
    public void fanoutExchangeRoutesMessagesRoundRobin() throws InterruptedException {
        byte[] messageBody = "fanout message".getBytes();
        Message message = MessageBuilder.withBody(messageBody).build();

        TestMessageListener listener1 = createAndBindMessageListener("fanout-declareQueue-round-robin");
        TestMessageListener listener2 = createAndBindMessageListener("fanout-declareQueue-round-robin");
        TestMessageListener listener3 = createAndBindMessageListener("fanout-declareQueue-round-robin");

        template.convertAndSend(message);
        template.convertAndSend(message);
        template.convertAndSend(message);

        listener1.latch.await(1, TimeUnit.SECONDS);
        listener2.latch.await(1, TimeUnit.SECONDS);
        listener3.latch.await(1, TimeUnit.SECONDS);

        assertThat(listener1.receivedMessages.size(), is(1));
        assertThat(listener2.receivedMessages.size(), is(1));
        assertThat(listener3.receivedMessages.size(), is(1));

        assertThat(listener1.receivedMessages.get(0).getBody(), is(messageBody));
        assertThat(listener2.receivedMessages.get(0).getBody(), is(messageBody));
        assertThat(listener3.receivedMessages.get(0).getBody(), is(messageBody));
    }

    private TestMessageListener createAndBindMessageListener(String queueName) {
        TestMessageListener listener = new TestMessageListener();
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(declareQueue(queueName));
        container.setMessageListener(listener);
        container.start();
        return listener;
    }

    public Queue declareQueue(String queueName) {
        Queue queue = new Queue(queueName, false, false, true);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(fanoutExchange));
        return queue;
    }

    @Configuration
    @Import(SimpleRabbitServerConfiguration.class)
    public static class TestConfiguration {

        private static final String FANOUT_EXCHANGE = "ExchangeTypesTests.FanoutExchange";

        @Autowired
        private RabbitAdmin rabbitAdmin;

        @Autowired
        private ConnectionFactory connectionFactory;

        @Bean
        public FanoutExchange fanoutExchange() {
            FanoutExchange exchange = new FanoutExchange(FANOUT_EXCHANGE);
            rabbitAdmin.declareExchange(exchange);
            return exchange;
        }

        @Bean
        public RabbitTemplate rabbitTemplate() {
            RabbitTemplate template = new RabbitTemplate(connectionFactory);
            template.setExchange(FANOUT_EXCHANGE);
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
