import zmq
import msgpack
import time

context = zmq.Context()

# recebe dos bots
frontend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:8000")

# comunica com os servidores
servidor1 = context.socket(zmq.REQ)
servidor1.bind("tcp://*:8001")

servidor2 = context.socket(zmq.REQ)
servidor2.bind("tcp://*:8002")

servidores = [servidor1, servidor2]

print("Broker iniciado", flush=True)

while True:
    partes = frontend.recv_multipart()
    mensagem = partes[-1]
    dados = msgpack.unpackb(mensagem, raw=False)

    tipo = dados["tipo"]

    if tipo == "criar_canal":
        respostas = []

        for servidor in servidores:
            servidor.send(mensagem)
            respostas.append(msgpack.unpackb(servidor.recv(), raw=False))

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
            canais.update(resposta_servidor.get("canais", []))

        resposta = {
            "status": "sucesso",
            "mensagem": "Lista de canais",
            "timestamp": time.time(),
            "canais": sorted(canais)
        }

    else:
        servidor1.send(mensagem)
        resposta = msgpack.unpackb(servidor1.recv(), raw=False)

    frontend.send_multipart([
        partes[0],
        b"",
        msgpack.packb(resposta)
    ])