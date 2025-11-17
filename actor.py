import zmq
import time
import sys


# Configuración

GC_IP = "tcp://10.43.103.177:5556"  # IP del GC (PUB)
TOPICO = b"devolucion"  # valor por defecto, se puede cambiar por argumento


# Ejecución principal
if __name__ == "__main__":
    # Permitir seleccionar el tópico como argumento
    # Ejemplo: python actor.py renovacion
    if len(sys.argv) > 1:
        TOPICO = sys.argv[1].encode("utf-8")

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(GC_IP)
    socket.setsockopt(zmq.SUBSCRIBE, TOPICO)

    print(f"Actor suscrito al canal '{TOPICO.decode()}' del Gestor de Carga...")
    print(f"Esperando mensajes...\n")

    while True:
        # Recibir mensaje del canal
        mensaje = socket.recv_string()
        # El mensaje llega como: "devolucion user1,ElQuijote"
        topico, contenido = mensaje.split(" ", 1)
        usuario, libro = contenido.split(",", 1)

        # Simular procesamiento
        print(f" [{topico.upper()}] Usuario: {usuario} | Libro: {libro}")
        time.sleep(0.5)  # simulación de acción (guardar, confirmar, etc.)
