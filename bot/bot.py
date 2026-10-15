import zmq
import msgpack
import time

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:8000")

nome = "Luanna"

while True:

    # mensagem de login
    mensagem = {
        "tipo": "login",
        "nome": nome,
        "timestamp": time.time()
    }

    # Mensagem vira binário e é enviada
    dados = msgpack.packb(mensagem)
    socket.send(dados)

    print(f"Bot {nome} enviou login: {mensagem}")

    # Espera resposta do servidor
    resposta_bytes = socket.recv()

    # Resposta binária vira dicionário
    resposta = msgpack.unpackb(resposta_bytes, raw=False)

    print(f"Bot {nome} recebeu: {resposta}")

    if resposta["status"] == "sucesso":
        print(f"Seu login foi realizado com sucesso! Bem-vinda, {nome}!")
        break

    else:
        print(f"Erro: {resposta.get('mensagem', 'Erro desconhecido')}")
        print("Tentando novamente em 10 segundos...")
        time.sleep(10)
