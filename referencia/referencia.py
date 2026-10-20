import zmq
import msgpack
import time

contexto = zmq.Context()

socket = contexto.socket(zmq.REP)
socket.bind("tcp://*:6000")

servidores = {}
proximo_rank = 1

print("Servico de referencia iniciado na porta 6000", flush=True)

while True:
    dados = socket.recv()
    mensagem = msgpack.unpackb(dados, raw=False)

    tipo = mensagem.get("tipo")
    nome = mensagem.get("nome")

    if tipo == "rank":
        if nome not in servidores:
            servidores[nome] = {
                "rank": proximo_rank,
                "ultimo_heartbeat": time.time()
            }
            proximo_rank += 1

        resposta = {
            "status": "sucesso",
            "rank": servidores[nome]["rank"],
            "timestamp": time.time()
        }

    elif tipo == "listar":
        lista = []

        for nome_servidor, dados_servidor in servidores.items():
            lista.append({
                "nome": nome_servidor,
                "rank": dados_servidor["rank"]
            })

        resposta = {
            "status": "sucesso",
            "servidores": lista,
            "timestamp": time.time()
        }

    elif tipo == "heartbeat":
        if nome in servidores:
            servidores[nome]["ultimo_heartbeat"] = time.time()

            resposta = {
                "status": "sucesso",
                "mensagem": "OK",
                "timestamp": time.time()
            }
        else:
            resposta = {
                "status": "erro",
                "mensagem": "Servidor nao cadastrado",
                "timestamp": time.time()
            }

    else:
        resposta = {
            "status": "erro",
            "mensagem": "Tipo de mensagem desconhecido",
            "timestamp": time.time()
        }

    socket.send(msgpack.packb(resposta))