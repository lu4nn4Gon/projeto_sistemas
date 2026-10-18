import zmq
import msgpack
import time
import os
import random

nome = os.getenv("BOT_NOME", "Luanna")
canal = os.getenv("BOT_CANAL", "geral")

contexto = zmq.Context()

#REQ/REP com o broker
socket = contexto.socket(zmq.REQ)
socket.connect("tcp://broker:8000")

#subscriber
assinante = contexto.socket(zmq.SUB)
assinante.connect("tcp://proxy:5558")


# enviando mensagem ao broker
def enviar(tipo, nome=""):
    mensagem = {
        "tipo": tipo,
        "nome": nome,
        "timestamp": time.time()
    }

    dados = msgpack.packb(mensagem)
    socket.send(dados)

    print(f"Bot enviou: {mensagem}", flush=True)

    resposta_bytes = socket.recv()
    resposta = msgpack.unpackb(resposta_bytes, raw=False)

    print(f"Bot recebeu: {resposta}", flush=True)

    return resposta


# efetuando login
while True:
    resposta = enviar("login", nome)

    if resposta["status"] == "sucesso":
        print(f"Login realizado com sucesso! Bem-vinda, {nome}!", flush=True)
        break

    print("Erro no login. Tentando novamente em 10 segundos...", flush=True)
    time.sleep(10)


# mostrando os canais existentes
resposta = enviar("listar_canais")

if resposta["status"] == "sucesso":
    print("Canais existentes:", resposta.get("canais", []), flush=True)


# criando um canal
if canal.strip() != "":
    resposta = enviar("criar_canal", canal)

    if resposta["status"] == "sucesso":
        print(f"Canal {canal} criado com sucesso!", flush=True)
    else:
        print(resposta["mensagem"], flush=True)


# mostra os canais atualizados
resposta = enviar("listar_canais")

canais = []

if resposta["status"] == "sucesso":
    canais = resposta.get("canais", [])
    print("Canais disponíveis:", canais, flush=True)


# inscrevendo o bot em até 3 canais aleatórios
quantidade = min(3, len(canais))

inscricoes = random.sample(canais, quantidade)

for nome_canal in inscricoes:
    assinante.setsockopt_string(zmq.SUBSCRIBE, nome_canal + " ")
    print(f"{nome} se inscreveu no canal: {nome_canal}", flush=True)


# aguardando publicações
print(f"{nome} aguardando mensagens...", flush=True)

try:
    while True:
        partes = assinante.recv_multipart()

        if len(partes) != 2:
            print("Publicação em formato inesperado", flush=True)
            continue

        topico, dados = partes
        mensagem = msgpack.unpackb(dados, raw=False)

        print(
            f"[{topico.decode().strip()}] "
            f"{mensagem.get('mensagem')} | "
            f"Enviada: {mensagem.get('timestamp')} | "
            f"Recebida: {time.time()}",
            flush=True
        )

except KeyboardInterrupt:
    print(f"{nome} encerrando...", flush=True)

finally:
    assinante.close()
    socket.close()
    contexto.term()