
import zmq
import time


# Configuración de sockets

PUERTO_REQ_REP = "5555"   # recibe solicitudes del PS
PUERTO_PUB_SUB = "5556"   # publica mensajes a los Actores

# Crear contexto de ZeroMQ
context = zmq.Context()

# --- Socket REP: comunicación con PS ---
socket_rep = context.socket(zmq.REP)
socket_rep.bind(f"tcp://*:{PUERTO_REQ_REP}")

# --- Socket PUB: comunicación con Actores ---
socket_pub = context.socket(zmq.PUB)
socket_pub.bind(f"tcp://*:{PUERTO_PUB_SUB}")

print(f" Gestor de Carga iniciado.")
print(f" Esperando solicitudes de PS en puerto {PUERTO_REQ_REP}...")
print(f" Publicando mensajes para Actores en puerto {PUERTO_PUB_SUB}...\n")

while True:
    # Recibir solicitud del PS
    mensaje = socket_rep.recv_string()
    print(f" Solicitud recibida: {mensaje}")

    # Confirmar al PS que se recibió correctamente
    socket_rep.send_string("Solicitud recibida correctamente")

    # Parsear el mensaje
    try:
        tipo, usuario, libro = mensaje.split(",")
        tipo = tipo.strip().lower()
    except ValueError:
        print(" Formato de mensaje inválido.")
        continue

    # Publicar al canal correspondiente
    if tipo == "devolucion":
        socket_pub.send_string(f"devolucion {usuario},{libro}")
        print(f" Publicado en canal 'devolucion': {usuario},{libro}")
    elif tipo == "renovacion":
        socket_pub.send_string(f"renovacion {usuario},{libro}")
        print(f" Publicado en canal 'renovacion': {usuario},{libro}")
    else:
        print(f" Tipo de solicitud desconocido: {tipo}")

    time.sleep(0.2)  # pequeña pausa para evitar saturación
