package ch.lihsmi.spring.amqp.byexample.basics;

import ch.lihsmi.spring.amqp.byexample.config.SimpleRabbitServerConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SimpleRabbitServerConfiguration.class)
public class SpringMessageModelTest {

    @Autowired
    RabbitTemplate template;

    @Test
    public void sendAndReceiveMessageObject() {
        Message message = MessageBuilder.withBody("message body".getBytes())
                .setContentEncoding("text")
                .setHeader("header", "value")
                .build();

        template.convertAndSend("myqueue", message);
        Message received = template.receive("myqueue");

        assertThat(received.getBody(), is("message body".getBytes()));
        assertThat(received.getMessageProperties().getContentEncoding(), is("text"));
        assertThat(received.getMessageProperties().getHeaders().get("header"), is("value"));
    }

}
