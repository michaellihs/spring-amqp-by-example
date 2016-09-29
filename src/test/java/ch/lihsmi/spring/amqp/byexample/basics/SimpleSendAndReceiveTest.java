package ch.lihsmi.spring.amqp.byexample.basics;

import ch.lihsmi.spring.amqp.byexample.config.SimpleRabbitServerConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SimpleRabbitServerConfiguration.class)
public class SimpleSendAndReceiveTest {

    @Autowired
    private RabbitTemplate template;

    @Test
    public void messageCanBeSentAndConsumedAsExpected() {
        template.convertAndSend("myqueue", "foo");
        String foo = (String) template.receiveAndConvert("myqueue");

        assertThat(foo, is("foo"));
    }

}
