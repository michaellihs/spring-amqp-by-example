Spring AMQP by Example
======================

This project holds some code samples, snippets, tests and documentation for the [Spring AMQP library](https://projects.spring.io/spring-amqp/).


Prerequisites
-------------

The tests in this project require a locally running Rabbit MQ broker. The easiest way to set it up is using a Docker container. Once you have Docker running on your machine, use the following commands to run Rabbit MQ in Docker:

```sh
rmq_container_id=$(docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq) && sleep 5 && docker exec $rmq_container_id rabbitmq-plugins enable rabbitmq_management
```

