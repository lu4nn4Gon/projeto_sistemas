import zmq

contexto = zmq.Context()

# recebe as publicações dos servidores
xsub = contexto.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

# distribui as publicações aos bots inscritos
xpub = contexto.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("Proxy Pub/Sub iniciado", flush=True)
print("XSUB na porta 5557", flush=True)
print("XPUB na porta 5558", flush=True)

try:
    zmq.proxy(xsub, xpub)
except KeyboardInterrupt:
    print("Encerrando proxy...", flush=True)
finally:
    xsub.close()
    xpub.close()
    contexto.term()