<?php

require_once('./KafkaCustomConsumer.php');

$consumer = new \KafkaCustomConsumer('kafka:9092', 'default', 'group-test');

$consumer->startConsumer();
