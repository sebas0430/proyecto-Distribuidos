


import zmq
import time
import random
import sys


#configuracion
GC_IP = "tcp://10.43.103.177:5555"  # IP real del Gestor de Carga
ARCHIVO_SOLICITUDES = "solicitudes.txt"

#leer solicitudes de un txt (devolucion,user1,ElQuijote)
def leer_solicitudes(nombre_archivo):
    solicitudes = []
    try:
        with open(nombre_archivo, 'r') as archivo:
            for linea in archivo:
                partes = linea.strip().split(',')
                if len(partes) == 3:
                    tipo_solicitud, usuario, libro = partes
                    solicitudes.append((tipo_solicitud, usuario, libro))
    except FileNotFoundError:
        print(f"El archivo {nombre_archivo} no fue encontrado.")
    return solicitudes

#enviar solicitud al gestor de carga
def enviar_solicitud(solicitudes):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(GC_IP)

    print("Iniciando proceso de solicitudes...")
    for solicitud in solicitudes:
        tipo_solicitud, usuario, libro = solicitud
        mensaje = f"{tipo_solicitud},{usuario},{libro}"
        print(f"Enviando solicitud: {mensaje}")
        socket.send_string(mensaje)

        # Esperar respuesta del Gestor de Carga
        respuesta = socket.recv_string()
        print(f"Respuesta del Gestor de Carga: {respuesta}")

        # Simular tiempo entre solicitudes
        time.sleep(random.uniform(0.5, 2.0))

    socket.close()
    context.term()


if __name__ == "__main__":
    solicitudes = leer_solicitudes(ARCHIVO_SOLICITUDES)
    if not solicitudes:
        print("No hay solicitudes para procesar.")
        sys.exit(0)

    enviar_solicitud(solicitudes)
    print("Todas las solicitudes han sido procesadas.")


