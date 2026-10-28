import zmq
import msgpack
import time

contexto = zmq.Context()

socket = contexto.socket(zmq.REP)
socket.bind("tcp://*:6000")

servidores = {}
proximo_rank = 1

TEMPO_LIMITE = 30

# relogio logico da referencia
relogio_logico = 0

print("Servico de referencia iniciado na porta 6000", flush=True)

while True:
    agora = time.time()

    for nome_servidor, dados_servidor in list(servidores.items()):
        if agora - dados_servidor["ultimo_heartbeat"] > TEMPO_LIMITE:
            print(
                f"Servidor {nome_servidor} removido por falta de heartbeat",
                flush=True
            )
            del servidores[nome_servidor]

    if socket.poll(1000) == 0:
        continue

    dados = socket.recv()
    mensagem = msgpack.unpackb(dados, raw=False)

    # atualiza o relogio logico
    contador_recebido = mensagem.get("relogio_logico", 0)
    relogio_logico = max(relogio_logico, contador_recebido)

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
                "mensagem": "OK"
            }
        else:
            resposta = {
                "status": "erro",
                "mensagem": "Servidor nao cadastrado"
            }

    else:
        resposta = {
            "status": "erro",
            "mensagem": "Tipo de mensagem desconhecido",
            "timestamp": time.time()
        }

    # incrementa o relogio antes de enviar
    relogio_logico += 1
    resposta["relogio_logico"] = relogio_logico

    # envia a resposta usando MessagePack
    socket.send(msgpack.packb(resposta, use_bin_type=True))