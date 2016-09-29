Spring AMQP by Example
======================

This project holds some code samples, snippets, tests and documentation for the [Spring AMQP library](https://projects.spring.io/spring-amqp/).


Prerequisites
-------------

The tests in this project require a locally running Rabbit MQ broker. The easiest way to set it up is using a Docker container. Once you have Docker running on your machine, use the following commands to run Rabbit MQ in Docker:

```sh
rmq_container_id=$(docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq) && sleep 5 && docker exec $rmq_container_id rabbitmq-plugins enable rabbitmq_management
```


Collection of Examples
======================

To make the examples easier to be run separately, each is written as a test within the `src/test/java` directory.

| Example | Description |
| ------- | ----------- |
| [Simple send and receive](src/test/java/ch/lihsmi/spring/amqp/byexample/basics/SimpleSendAndReceiveTest.java) | A very basic example to send an receive messages with `RabbitTemplate` |
| [Spring Message Class](src/test/java/ch/lihsmi/spring/amqp/byexample/basics/SpringMessageModelTest.java) | Using the Spring `Message` abstraction to send and receive messages |


Further Resources
=================

* [Spring AMQP Reference](http://docs.spring.io/spring-amqp/reference/html/)
* [(Book) RabbitMQ Essentials](https://www.packtpub.com/application-development/rabbitmq-essentials)