# PHP Kafta - Exemplo

Projeto PHP voltado a exemplificar a produção e o consumo de mensagens de um tópico no Kafka.
Neste projeto também foi desenvolvido um mecanismo de retentativa de consumo e, esgotando-se as retentativas, as mensagens serão enviadas para um tópico DLQ (dead letter queue).

## Referências

 - [PHP Rdkafka](https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/book.rdkafka.html)
 - [Kafka](https://kafka.apache.org/documentation)
 - [Fonte](https://www.youtube.com/watch?v=7ZrGVpExBHU)

## Run

Suba os containers do projeto com o docker-compose:

```
docker-compose up -d
```

Acesse o log do consumer:

```
docker-compose exec php-kafka php producer.php
```

Execute o producer:

```
docker-compose exec php-kafka php producer.php
```

Execute o seguinte comando para consultar o consumo do grupo no tópico:

```
docker-compose exec kafka kafka-consumer-groups --describe --group group-test --bootstrap-server kafka:9092
```

Para acessar o Control Center da Conluent para gerenciar o cluster do Kafka: http://localhost:9021/

Caso faça alguma modificação nos scripts PHP com o container rodando, execute os seguintes comandos para restartar o container da aplicação:

```
docker-compose build php-kafka \
&& docker-compose restart php-kafka \
&& docker-compose logs -f php-kafka
```