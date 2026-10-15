import zmq
import msgpack
import time
import os

nome = os.getenv("BOT_NOME", "Luanna")
canal = os.getenv("BOT_CANAL", "geral")

contexto = zmq.Context()
socket = contexto.socket(zmq.REQ)
socket.connect("tcp://broker:8000")


# enviando mensagem
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


if nome.strip() == "":
    print("Erro: nome vazio")
    exit()


# efetuando login
while True:
    resposta = enviar("login", nome)

    if resposta["status"] == "sucesso":
        print(f"Login realizado com sucesso! Bem-vinda, {nome}!")
        break

    print("Erro no login. Tentando novamente em 10 segundos...")
    time.sleep(10)


# mostrando os canais existentes
resposta = enviar("listar_canais")

if resposta["status"] == "sucesso":
    print("Canais existentes:", resposta.get("canais", []))


# criando um canal
if canal.strip() != "":
    resposta = enviar("criar_canal", canal)

    if resposta["status"] == "sucesso":
        print(f"Canal {canal} criado com sucesso!")
    else:
        print(resposta["mensagem"])

resposta = enviar("listar_canais")

if resposta["status"] == "sucesso":
    print("Canais disponíveis:", resposta.get("canais", []))


socket.close()
contexto.term()
