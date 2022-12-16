<?php

class KafkaConsumerHandler
{
    public static function handle(?string $key, string $payload): void
    {
        if ($payload === "x") {
            throw new \Exception('X não pode!');
        }

        var_dump($key, $payload);
    }
}
