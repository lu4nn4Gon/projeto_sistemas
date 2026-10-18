# Projeto de Sistemas Distribuídos

## Introdução


## Tecnologias utilizadas

- **Python:** utilizado para desenvolver os bots e o broker.
- **C:** utilizado para desenvolver os servidores.
- **ZeroMQ:** utilizado para a comunicação entre os componentes.
- **MessagePack:** utilizado para serializar as mensagens em formato binário.
- **Docker Compose:** utilizado para executar os containers.

Optei pelo Python para os bots e o broker pela facilidade de implementação. A linguagem C foi utilizada para desenvolver os servidores.

## Funcionamento

O sistema possui quatro bots, dois servidores e um broker.

Os bots enviam solicitações ao broker, que encaminha as mensagens aos servidores e devolve as respostas aos bots.

Todas as mensagens são serializadas com MessagePack e possuem um timestamp indicando o momento do envio.

### Login

Ao iniciar, cada bot envia seu nome para realizar o login.

O servidor verifica o nome e responde informando se o login foi realizado com sucesso ou se ocorreu algum erro.

Caso receba uma resposta de erro, o bot aguarda 10 segundos antes de tentar novamente.

### Criação de canais

Após realizar o login, o bot solicita a lista dos canais disponíveis.

O bot também pode solicitar a criação de um canal.

O broker encaminha a solicitação aos dois servidores, que verificam se o canal já existe.

Se o canal não existir, ele será criado. Caso já exista, o sistema retorna uma mensagem de erro.

### Listagem de canais

O bot solicita a lista de canais ao broker.

O broker consulta os dois servidores, reúne os nomes dos canais e devolve a lista ao bot.

## Persistência dos dados

Os servidores armazenam as informações em dois arquivos:

- `logins.txt`: armazena os nomes dos usuários que realizaram login e seus timestamps.
- `canais.txt`: armazena os nomes dos canais criados e seus timestamps.

Cada servidor possui seu próprio volume Docker, mantendo os arquivos separados.

Os dados continuam armazenados após a reinicialização dos containers, desde que os volumes não sejam removidos.

## Como executar

Para executar o projeto, é necessário ter Docker e Docker Compose instalados.

Na pasta principal do projeto, execute:

```bash
docker compose up --build
```

