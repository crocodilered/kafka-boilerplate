# kafka-boilerplate

Simple code for working with Kafka brokers using <a href="https://aiokafka.readthedocs.io/en/stable/">aiokafka</a>. 

To see something you should run docker-compose, create topic inside of Kafka, then run `python consumer.py <topic_name> g1`, leave it and in another window run `python producer.py <topic_name>`.

The point is to write common code with single point of brokers behavior configuration.
