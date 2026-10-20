# Projeto de Sistemas Distribuídos

## Introdução


## Tecnologias utilizadas

- **Python:** utilizado para desenvolver os bots, o broker e o proxy Pub/Sub.
- **C:** utilizado para desenvolver os servidores.
- **ZeroMQ:** utilizado para a comunicação entre os componentes.
- **MessagePack:** utilizado para serializar as mensagens em formato binário.
- **Docker Compose:** utilizado para executar os containers.


## Funcionamento

O sistema possui quatro bots, dois servidores, um broker e um proxy Pub/Sub.

Os bots enviam solicitações ao broker, que encaminha as mensagens aos servidores e devolve as respostas aos bots.

O proxy Pub/Sub é responsável por distribuir as publicações aos bots inscritos nos canais.

Todas as mensagens transmitidas entre os componentes são serializadas com MessagePack e possuem um timestamp.

### Login

Ao iniciar, cada bot envia seu nome para realizar o login.

O servidor verifica o nome e responde informando se o login foi realizado com sucesso ou se ocorreu algum erro.

Caso receba uma resposta de erro, o bot aguarda 10 segundos antes de tentar novamente.

### Criação de canais

Após realizar o login, o bot solicita a lista dos canais disponíveis.

Se existirem menos de cinco canais, o bot tenta criar um novo canal.

O broker encaminha a solicitação aos dois servidores, que verificam se o canal já existe.

Se o canal não existir, ele será criado. Caso já exista, o sistema retorna uma mensagem de erro.

### Listagem de canais

O bot solicita a lista de canais ao broker.

O broker consulta os dois servidores, reúne os nomes dos canais e devolve a lista ao bot.

### Inscrição nos canais

Após consultar os canais disponíveis, cada bot escolhe aleatoriamente até três canais para se inscrever.

As inscrições são realizadas utilizando um socket SUB do ZeroMQ.

Cada bot pode receber mensagens de diferentes canais utilizando a mesma conexão.

### Publicação de mensagens

Após realizar as inscrições, os bots começam a publicar mensagens automaticamente.

Cada bot escolhe aleatoriamente um canal disponível e envia dez mensagens, com intervalo de um segundo entre elas.

Após as dez mensagens, o processo é repetido continuamente.

As publicações são enviadas ao broker utilizando a comunicação REQ/REP.

O broker distribui as solicitações de publicação entre os dois servidores utilizando Round Robin.

O servidor responsável pela publicação salva a mensagem em disco e a encaminha ao proxy Pub/Sub.

### Recebimento de mensagens

O proxy Pub/Sub utiliza duas portas:

- **5557 (XSUB):** recebe as publicações dos servidores.
- **5558 (XPUB):** distribui as publicações aos bots inscritos.

Os bots utilizam uma thread para receber mensagens enquanto continuam realizando publicações.

Ao receber uma mensagem, o bot exibe:

- Nome do canal.
- Conteúdo da mensagem.
- Timestamp de envio.
- Timestamp de recebimento.

Dessa forma, os bots conseguem publicar e receber mensagens simultaneamente.

## Persistência dos dados

Os servidores armazenam as informações em três arquivos:

- `logins.txt`: armazena os nomes dos usuários que realizaram login e seus timestamps.
- `canais.txt`: armazena os nomes dos canais criados e seus timestamps.
- `publicacoes.txt`: armazena as mensagens publicadas, com seus canais e timestamps.

Cada servidor possui seu próprio volume Docker, mantendo os arquivos separados.

Os dados continuam armazenados após a reinicialização dos containers, desde que os volumes não sejam removidos.

## Como executar

Para executar o projeto, é necessário ter Docker e Docker Compose instalados.

Na pasta principal do projeto, execute:

```bash
docker compose up --build
```

Para executar os containers em segundo plano:

```bash
docker compose up -d --build
```

Para acompanhar as mensagens de um bot:

```bash
docker compose logs -f bot1
```

Para consultar as publicações armazenadas nos servidores:

```bash
docker compose exec servidor1 tail /dados/publicacoes.txt
docker compose exec servidor2 tail /dados/publicacoes.txt
```

Para encerrar os containers:

```bash
docker compose down
```
