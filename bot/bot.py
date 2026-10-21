import zmq
import msgpack
import time
import os
import random
import threading

nome = os.getenv("BOT_NOME", "Luanna")
canal = os.getenv("BOT_CANAL", "geral")

contexto = zmq.Context()

relogio_logico = 0
trava_relogio = threading.Lock()


def incrementar_relogio():
    global relogio_logico

    with trava_relogio:
        relogio_logico += 1
        return relogio_logico


def atualizar_relogio(valor_recebido):
    global relogio_logico

    with trava_relogio:
        relogio_logico = max(relogio_logico, valor_recebido)
        return relogio_logico


#req/rep com o broker
socket = contexto.socket(zmq.REQ)
socket.connect("tcp://broker:8000")

#subscriber
assinante = contexto.socket(zmq.SUB)
assinante.connect("tcp://proxy:5558")


# enviando mensagem ao broker
def enviar(tipo, nome="", canal="", mensagem=""):
    contador = incrementar_relogio()

    dados_mensagem = {
        "tipo": tipo,
        "nome": nome,
        "timestamp": time.time(),
        "relogio_logico": contador
    }

    if tipo == "publicar":
        dados_mensagem["canal"] = canal
        dados_mensagem["mensagem"] = mensagem

    dados = msgpack.packb(dados_mensagem)
    socket.send(dados)

    print(f"Bot enviou: {dados_mensagem}", flush=True)

    resposta_bytes = socket.recv()
    resposta = msgpack.unpackb(resposta_bytes, raw=False)

    atualizar_relogio(resposta.get("relogio_logico", 0))

    print(f"Bot recebeu: {resposta}", flush=True)
    print(f"Relogio logico atual: {relogio_logico}", flush=True)

    return resposta


def receber_mensagens():
    while True:
        try:
            partes = assinante.recv_multipart()

            if len(partes) != 2:
                print("Publicação em formato inesperado", flush=True)
                continue

            topico, dados = partes
            mensagem = msgpack.unpackb(dados, raw=False)

            atualizar_relogio(mensagem.get("relogio_logico", 0))

            print(
                f"[{topico.decode().strip()}] "
                f"{mensagem.get('mensagem')} | "
                f"Enviada: {mensagem.get('timestamp')} | "
                f"Recebida: {time.time()} | "
                f"Relogio logico: {relogio_logico}",
                flush=True
            )

        except zmq.ZMQError:
            break


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

canais = []

if resposta["status"] == "sucesso":
    canais = resposta.get("canais", [])
    print("Canais existentes:", canais, flush=True)


# criando um canal
if len(canais) < 5 and canal.strip() != "":
    resposta = enviar("criar_canal", canal)

    if resposta["status"] == "sucesso":
        print(f"Canal {canal} criado com sucesso!", flush=True)
    else:
        print(resposta["mensagem"], flush=True)


# mostra os canais atualizados
resposta = enviar("listar_canais")

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

thread = threading.Thread(target=receber_mensagens, daemon=True)
thread.start()

mensagens = [
    "Ola pessoal!",
    "Tudo bem?",
    "Bom dia!",
    "Alguem por aqui?",
    "Testando o canal",
    "Mensagem automatica",
    "Sistemas distribuidos",
    "Mais uma mensagem",
    "Teste de publicacao",
    "Ate a proxima!"
]

try:
    while True:
        if not canais:
            time.sleep(1)
            continue

        canal_escolhido = random.choice(canais)

        print(f"{nome} vai publicar no canal: {canal_escolhido}", flush=True)

        for i in range(10):
            mensagem = random.choice(mensagens)

            resposta = enviar(
                "publicar",
                canal=canal_escolhido,
                mensagem=mensagem
            )

            if resposta["status"] != "sucesso":
                print("Erro ao publicar mensagem", flush=True)

            time.sleep(1)

except KeyboardInterrupt:
    print(f"{nome} encerrando...", flush=True)

finally:
    assinante.close()
    socket.close()
    contexto.term()