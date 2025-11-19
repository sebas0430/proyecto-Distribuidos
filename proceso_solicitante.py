import zmq
import time
import random
import sys
import json

def leer_solicitudes(nombre_archivo):
    """Lee solicitudes de un archivo txt (formato: devolucion,user1,ISBN0001)"""
    solicitudes = []
    try:
        with open(nombre_archivo, 'r', encoding='utf-8') as archivo:
            for linea in archivo:
                linea = linea.strip()
                if not linea or linea.startswith('#'):  # Ignorar líneas vacías y comentarios
                    continue
                    
                partes = linea.split(',')
                if len(partes) == 3:
                    tipo_solicitud, usuario, libro = partes
                    solicitudes.append((tipo_solicitud.strip(), usuario.strip(), libro.strip()))
                else:
                    print(f"⚠️  Línea ignorada (formato incorrecto): {linea}")
    except FileNotFoundError:
        print(f"❌ El archivo {nombre_archivo} no fue encontrado.")
    return solicitudes

def enviar_solicitud(solicitudes, gc_ip, nombre_ps="PS"):
    """Envía solicitudes al Gestor de Carga"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(gc_ip)
    
    print(f" [{nombre_ps}] Iniciando proceso de solicitudes a {gc_ip}...")
    print(f" Total de solicitudes: {len(solicitudes)}\n")
    
    exitosas = 0
    fallidas = 0
    tiempos = []
    
    for i, solicitud in enumerate(solicitudes, 1):
        tipo_solicitud, usuario, libro = solicitud
        mensaje = f"{tipo_solicitud},{usuario},{libro}"
        
        print(f"[{i}/{len(solicitudes)}] Enviando: {mensaje}")
        
        try:
            inicio = time.time()
            socket.send_string(mensaje)
            
            # Esperar respuesta del Gestor de Carga
            respuesta_json = socket.recv_string()
            fin = time.time()
            
            tiempo_respuesta = (fin - inicio) * 1000  # ms
            tiempos.append(tiempo_respuesta)
            
            respuesta = json.loads(respuesta_json)
            
            if respuesta.get("exito", False):
                print(f"✅ {respuesta['mensaje']} (Tiempo: {tiempo_respuesta:.2f}ms)\n")
                exitosas += 1
            else:
                print(f"❌ {respuesta['mensaje']} (Tiempo: {tiempo_respuesta:.2f}ms)\n")
                fallidas += 1
            
            # Simular tiempo entre solicitudes
            time.sleep(random.uniform(0.5, 2.0))
            
        except Exception as e:
            print(f"❌ Error enviando solicitud: {e}\n")
            fallidas += 1
    
    socket.close()
    context.term()
    
    # Resumen
    print("\n" + "="*60)
    print(f" RESUMEN [{nombre_ps}]")
    print("="*60)
    print(f"Total solicitudes: {len(solicitudes)}")
    print(f"✅ Exitosas: {exitosas}")
    print(f"❌ Fallidas: {fallidas}")
    if tiempos:
        print(f"⏱️  Tiempo promedio de respuesta: {sum(tiempos)/len(tiempos):.2f}ms")
        print(f"⏱️  Tiempo mínimo: {min(tiempos):.2f}ms")
        print(f"⏱️  Tiempo máximo: {max(tiempos):.2f}ms")
    print("="*60)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python proceso_solicitante.py <archivo> <gc_ip> <gc_puerto> [nombre_ps]")
        print("\nEjemplos:")
        print("  Sede 1: python proceso_solicitante.py solicitudes.txt 10.43.103.177 5555 PS_Sede1")
        print("  Sede 2: python proceso_solicitante.py solicitudes_sede2.txt 10.43.103.132 5565 PS_Sede2")
        sys.exit(1)
    
    ARCHIVO_SOLICITUDES = sys.argv[1]
    GC_IP = f"tcp://{sys.argv[2]}:{sys.argv[3]}"
    NOMBRE_PS = sys.argv[4] if len(sys.argv) > 4 else "PS"
    
    solicitudes = leer_solicitudes(ARCHIVO_SOLICITUDES)
    if not solicitudes:
        print("❌ No hay solicitudes para procesar.")
        sys.exit(0)

    enviar_solicitud(solicitudes, GC_IP, NOMBRE_PS)
    print(f"\n✅ [{NOMBRE_PS}] Todas las solicitudes han sido procesadas.")