# Implementação da atividade: Introdução a Publish-Subscribe com Kafka

Esta implementação cria o fluxo pedido no enunciado:

```text
producer(topic1) --> consumer(topic1)/producer(topic2) --> consumer(topic2)
```

## Arquivos

- `const.py`: endereço e porta do broker Kafka.
- `producer.py`: publica mensagens em um tópico informado pela linha de comando.
- `consumer_processor.py`: consome mensagens de um tópico de entrada, processa cada evento e publica o resultado em um tópico de saída.
- `consumer.py`: consome e imprime mensagens de um tópico.

## Como executar

Abra três terminais, com o ambiente Python configurado e o Kafka em execução.

### Terminal 1: consumidor final

```bash
python3 consumer.py topic2
```

Esse programa ficará esperando mensagens processadas no `topic2`.

### Terminal 2: consumidor intermediário e produtor secundário

```bash
python3 consumer_processor.py topic1 topic2
```

Esse programa lê mensagens do `topic1`, processa cada uma e publica o resultado no `topic2`.

### Terminal 3: produtor inicial

```bash
python3 producer.py topic1
```

Esse programa envia 100 mensagens para o `topic1`.

## Resultado esperado

O produtor inicial envia mensagens para o `topic1`.
O processador recebe essas mensagens, transforma o texto para maiúsculas e adiciona o prefixo `Processed event:`.
O consumidor final recebe as mensagens processadas no `topic2`.
