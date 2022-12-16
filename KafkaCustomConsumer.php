<?php

require_once('./KafkaConsumerHandler.php');
require_once('./KafkaDLQProducer.php');

class KafkaCustomConsumer
{
    private $conf;
    private $consumer;
    private $topicName;
    private $dlq;
    private $isActive;
    private $retries;
    private $retryFactory;
    private $initialRetryTime;
    private $retryMultiplier;

    public function __construct(string $bootstrapServer, string $topicName, string $groupId)
    {
        $this->conf = new \RdKafka\Conf();

        $this->conf->set('bootstrap.servers', $bootstrapServer);
        // $this->conf->set('security.protocol', 'SASL_SSL');
        $this->conf->set('security.protocol', 'PLAINTEXT');
        $this->conf->set('sasl.mechanism', 'PLAIN');
        // $conf->set('sasl.username', '');
        // $conf->set('sasl.password', '');
        $this->conf->set('group.id', $groupId);
        $this->conf->set('auto.offset.reset', 'earliest');
        $this->conf->set('enable.auto.commit', 'false');
        $this->conf->set('enable.partition.eof', 'true');
        $this->conf->set('heartbeat.interval.ms', 15000);
        $this->conf->set('session.timeout.ms', 45000);

        $this->conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign: ";
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;
        
                 case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                     echo "Revoke: ";
                     var_dump($partitions);
                     $kafka->assign(NULL);
                     break;
        
                 default:
                    throw new \Exception($err);
            }
        });

        $this->topicName = $topicName;
        $this->isActive = false;
        $this->retries = 3;
        $this->retryFactory = .2;
        $this->initialRetryTime = 3000;
        $this->retryMultiplier = 2;
        
        $this->dlq = new \KafkaDLQProducer($bootstrapServer, "$this->topicName-dlq");
        
        $this->consumer = new \RdKafka\KafkaConsumer($this->conf);
        
        $this->consumer->subscribe([$this->topicName]);
        
    }

    public function startConsumer(): void
    {
        $previousRetryTime = $this->initialRetryTime;
        $nRetry = 0;
        $this->isActive = true;
        while ($this->isActive) {
            $message = $this->consumer->consume(5000);
        
            try {
        
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        \KafkaConsumerHandler::handle($message->key, $message->payload);
                        $this->consumer->commit($message);
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        echo "No more messages; will wait for more\n";
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        echo "waiting...\n";
                        break;
                    default:
                        throw new \Exception($message->errstr(), $message->err);
                        break;
                }
            } catch (Exception $e) {
                echo "[ERROR] ", $e->getMessage(), "\n";
                $this->consumer->close();

                if ($nRetry < $this->retries) {
                    if ($nRetry > 0) {
                        $previousRetryTime = rand($previousRetryTime * (1 - $this->retryFactory), $previousRetryTime * (1 + $this->retryFactory)) * $this->retryMultiplier;
                    }
                    echo "[RETRY] Topic $message->topic_name, partition $message->partition, offset $message->offset, key $message->key: Will try again in $previousRetryTime milliseconds\n";
                    sleep($previousRetryTime/1000);
                    $this->consumer = new \RdKafka\KafkaConsumer($this->conf);
                    $this->consumer->subscribe([$this->topicName]);
                    ++$nRetry;
                }
        
                if ($nRetry === $this->retries) {
                    $this->dlq->produce($message->key, $message->payload);
                    echo "[STOPED] Topic $message->topic_name, partition $message->partition, offset $message->offset, key $message->key: The retries are over\n";
                    throw $e;
                }
            }
        }
    }

    public function stopConsumer()
    {
        $this->isActive = false;
    }
}
