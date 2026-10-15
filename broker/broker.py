import zmq

context = zmq.Context()

#recebe dos bots
frontend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:8000")

# manda pros servidores
backend = context.socket(zmq.DEALER)
backend.bind("tcp://*:8001")

print("Broker foi iniciado nas portas 8000 e 8001", flush=True)

zmq.proxy(frontend, backend)
