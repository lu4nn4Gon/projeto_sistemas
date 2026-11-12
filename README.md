# Projeto de Sistemas Distribuídos

## Introdução

Este projeto foi desenvolvido para a disciplina de Sistemas Distribuídos.

O objetivo é implementar um sistema de comunicação distribuído, no qual bots podem realizar login, criar canais, publicar e receber mensagens.

## Tecnologias utilizadas

- **Python:** utilizado para desenvolver os bots, o broker, o proxy Pub/Sub e o serviço de referência.
- **C:** utilizado para desenvolver os servidores.
- **ZeroMQ:** utilizado para a comunicação entre os componentes.
- **MessagePack:** utilizado para serializar as mensagens em formato binário.
- **Docker Compose:** utilizado para executar os containers.

## Funcionamento

O sistema possui quatro bots, dois servidores, um broker, um proxy Pub/Sub e um serviço de referência, totalizando nove containers.

Os bots enviam solicitações ao broker, que encaminha as mensagens aos servidores e devolve as respostas aos bots.

O proxy Pub/Sub distribui as publicações aos bots inscritos nos canais.

Todas as mensagens transmitidas entre os componentes são serializadas com MessagePack e possuem um timestamp.

## Funcionalidades implementadas

- Login dos bots com tentativa automática após 10 segundos em caso de erro.
- Criação de canais.
- Listagem dos canais disponíveis.
- Inscrição em até três canais aleatórios.
- Publicação automática de mensagens.
- Recebimento de mensagens utilizando Pub/Sub.
- Publicação e recebimento simultâneos utilizando threads.
- Distribuição das publicações entre os servidores com Round Robin.
- Persistência de logins, canais e publicações.
- Relógios lógicos de Lamport.
- Cadastro dos servidores no serviço de referência.
- Monitoramento dos servidores utilizando heartbeat.
- Eleição de coordenador utilizando o algoritmo Bully.
- Sincronização de relógios utilizando o algoritmo Berkeley.
- Replicação de publicações entre os servidores.
- Confirmação e recuperação de replicações pendentes.

## Persistência dos dados

Os servidores armazenam as informações nos seguintes arquivos:

- `logins.txt`
- `canais.txt`
- `publicacoes.txt`

Cada servidor possui seu próprio volume Docker, permitindo preservar os dados após a reinicialização dos containers.

## Como executar

É necessário ter Docker e Docker Compose instalados.

Na pasta principal do projeto, execute:

```bash
docker compose up --build -d
docker compose ps
```

Para verificar os containers:

```bash
docker compose ps
```

Para acompanhar os logs:

```bash
docker compose logs -f
```

## Comandos para demonstração

### Visualizar os bots

```bash
docker compose logs -f --tail=30 bot1 bot2 bot3 bot4
```

### Visualizar o broker

```bash
docker compose logs -f --tail=30 broker
```

### Visualizar os servidores

```bash
docker compose logs -f --tail=30 servidor1 servidor2
```

### Visualizar o serviço de referência

```bash
docker compose logs -f --tail=50 referencia
```

### Consultar os canais criados

```bash
docker compose exec servidor1 cat /dados/canais.txt
```

### Consultar as publicações

Servidor 1:

```bash
docker compose exec servidor1 tail -n 20 /dados/publicacoes.txt
```

Servidor 2:

```bash
docker compose exec servidor2 tail -n 20 /dados/publicacoes.txt
```

### Demonstrar a persistência

Reiniciar o servidor:

```bash
docker compose restart servidor1
```

Verificar se os dados continuam armazenados:

```bash
docker compose exec servidor1 tail -n 20 /dados/publicacoes.txt
```

### Demonstrar a eleição Bully

Identificar nos logs qual servidor está atuando como coordenador.

```bash
docker compose logs --tail=500 servidor1 servidor2 | grep -Ei "coordenador|rank|BERKELEY"
```

Caso seja o servidor 2, interrompê-lo:

```bash
docker compose stop servidor2
```

Acompanhar os logs:

```bash
docker compose logs -f --tail=50 servidor1 referencia
```

Verificar se uma nova eleição foi realizada.

```bash
docker compose logs servidor1 servidor2 | grep -Ei 'eleicao|eleito|coordenador'
```

Iniciar novamente o servidor:

```bash
docker compose start servidor2
```

### Verificar a replicação

```bash
docker compose exec servidor1 tail -n 20 /dados/publicacoes.txt
docker compose exec servidor2 tail -n 20 /dados/publicacoes.txt
```

### Encerrar o projeto

```bash
docker compose down
```
