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
@SpringBootTest(classes = FanoutExchangeWithConfigurationAndListenerTest.TestConfiguration.class)
public class FanoutExchangeWithConfigurationAndListenerTest {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private TestMessageListener listener;

    @Test
    public void fanoutExchangeRoutesMessagesAsExpected() throws InterruptedException {
        byte[] messageBody = "fanout message".getBytes();
        Message message = MessageBuilder.withBody(messageBody).build();

        template.convertAndSend(message);
        listener.latch.await(1, TimeUnit.SECONDS);

        assertThat(listener.receivedMessages.get(0).getBody(), is(messageBody));
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
        public Queue queue(FanoutExchange exchange) {
            Queue queue = new Queue("fanout-queue", false, false, true);
            rabbitAdmin.declareQueue(queue);
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange));
            return queue;
        }

        @Bean
        public TestMessageListener listener(Queue queue) {
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
        }

    }

}
