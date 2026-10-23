import zmq
import msgpack
import time

context = zmq.Context()

relogio_logico = 0


def incrementar_relogio():
    global relogio_logico
    relogio_logico += 1
    return relogio_logico


def atualizar_relogio(valor_recebido):
    global relogio_logico
    relogio_logico = max(relogio_logico, valor_recebido)


# recebe dos bots
frontend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:8000")

# comunica com os servidores
servidor1 = context.socket(zmq.REQ)
servidor1.bind("tcp://*:8001")

servidor2 = context.socket(zmq.REQ)
servidor2.bind("tcp://*:8002")

servidores = [servidor1, servidor2]
proximo_servidor = 0

print("Broker iniciado", flush=True)

while True:
    partes = frontend.recv_multipart()
    mensagem = partes[-1]
    dados = msgpack.unpackb(mensagem, raw=False)

    atualizar_relogio(dados.get("relogio_logico", 0))

    tipo = dados["tipo"]

    if tipo == "criar_canal":
        respostas = []

        for servidor in servidores:
            servidor.send(mensagem)
            resposta_servidor = msgpack.unpackb(servidor.recv(), raw=False)

            atualizar_relogio(resposta_servidor.get("relogio_logico", 0))
            respostas.append(resposta_servidor)

        if all(r["status"] == "sucesso" for r in respostas):
            resposta = respostas[0]

        elif all(r["mensagem"] == "Canal já existe" for r in respostas):
            resposta = {
                "status": "erro",
                "mensagem": "Canal já existe",
                "timestamp": time.time()
            }

        else:
            resposta = {
                "status": "erro",
                "mensagem": "Erro ao criar canal em um ou mais servidores",
                "timestamp": time.time()
            }

    elif tipo == "listar_canais":
        canais = set()

        for servidor in servidores:
            servidor.send(mensagem)
            resposta_servidor = msgpack.unpackb(servidor.recv(), raw=False)

            atualizar_relogio(resposta_servidor.get("relogio_logico", 0))
            canais.update(resposta_servidor.get("canais", []))

        resposta = {
            "status": "sucesso",
            "mensagem": "Lista de canais",
            "timestamp": time.time(),
            "canais": sorted(canais)
        }

    elif tipo == "publicar":
        servidor = servidores[proximo_servidor]

        servidor.send(mensagem)
        resposta = msgpack.unpackb(servidor.recv(), raw=False)

        atualizar_relogio(resposta.get("relogio_logico", 0))

        proximo_servidor = (proximo_servidor + 1) % len(servidores)

    else:
        servidor1.send(mensagem)
        resposta = msgpack.unpackb(servidor1.recv(), raw=False)

        atualizar_relogio(resposta.get("relogio_logico", 0))

    # atualiza o relogio antes de responder ao bot
    resposta["relogio_logico"] = incrementar_relogio()

    frontend.send_multipart([
        partes[0],
        b"",
        msgpack.packb(resposta)
    ])