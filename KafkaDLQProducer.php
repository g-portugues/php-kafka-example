<?php

class KafkaDLQProducer
{
    private $producer;
    private $topic;

    public function __construct(string $bootstrapServer, string $topicName)
    {
        $conf = new \RdKafka\Conf();

        $conf->set('bootstrap.servers', $bootstrapServer);
        $conf->set('security.protocol', 'PLAINTEXT');
        $conf->set('sasl.mechanism', 'PLAIN');
        
        $this->producer = new \RdKafka\Producer($conf);
        $this->topic = $this->producer->newTopic($topicName);
    }

    public function produce(?string $key, string $payload): void
    {
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $key);
        $this->producer->flush(1000);
    }
}
