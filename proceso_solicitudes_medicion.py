import zmq
import time
import sys
import json
from datetime import datetime

def enviar_solicitudes_con_medicion(archivo, gc_ip, nombre_ps, duracion_segundos=120):
    """
    Envía solicitudes y captura métricas de rendimiento
    
    Args:
        archivo: archivo con solicitudes
        gc_ip: IP del gestor de carga (tcp://IP:puerto)
        nombre_ps: nombre del proceso solicitante
        duracion_segundos: duración máxima de la prueba (default 120s = 2min)
    """
    
    # Leer solicitudes
    solicitudes = []
    try:
        with open(archivo, 'r', encoding='utf-8') as f:
            for linea in f:
                linea = linea.strip()
                if linea and not linea.startswith('#'):
                    partes = linea.split(',')
                    if len(partes) == 3:
                        solicitudes.append(tuple(partes))
    except FileNotFoundError:
        print(f" Archivo {archivo} no encontrado")
        return
    
    if not solicitudes:
        print(f" No hay solicitudes en {archivo}")
        return
    
    # Conectar a GC
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(gc_ip)
    
    print(f" [{nombre_ps}] Iniciando medición")
    print(f" Solicitudes disponibles: {len(solicitudes)}")
    print(f"  Duración: {duracion_segundos}s")
    print(f" Destino: {gc_ip}\n")
    
    # Métricas
    tiempos_respuesta = []
    solicitudes_exitosas = 0
    solicitudes_fallidas = 0
    solicitudes_enviadas = 0
    
    # Control de tiempo
    tiempo_inicio = time.time()
    tiempo_limite = tiempo_inicio + duracion_segundos
    
    # Enviar solicitudes
    for tipo, usuario, libro in solicitudes:
        # Verificar si se acabó el tiempo
        if time.time() >= tiempo_limite:
            print(f" [{nombre_ps}] Tiempo límite alcanzado ({duracion_segundos}s)")
            break
        
        mensaje = f"{tipo},{usuario},{libro}"
        
        try:
            # Medir tiempo de respuesta
            inicio = time.time()
            socket.send_string(mensaje)
            respuesta_json = socket.recv_string()
            fin = time.time()
            
            tiempo_ms = (fin - inicio) * 1000
            tiempos_respuesta.append(tiempo_ms)
            solicitudes_enviadas += 1
            
            # Parsear respuesta
            respuesta = json.loads(respuesta_json)
            
            if respuesta.get("exito", False):
                solicitudes_exitosas += 1
                print(f"[{solicitudes_enviadas}]  {tiempo_ms:.2f}ms | {libro}")
            else:
                solicitudes_fallidas += 1
                print(f"[{solicitudes_enviadas}]  {tiempo_ms:.2f}ms | {respuesta.get('mensaje', 'Error')}")
        
        except Exception as e:
            solicitudes_fallidas += 1
            print(f"[{solicitudes_enviadas}]  Error: {e}")
        
        # Sin delay entre solicitudes para máxima carga
        # Si quieres simular usuarios reales, descomenta:
        # time.sleep(0.1)
    
    tiempo_total = time.time() - tiempo_inicio
    
    # Cerrar conexión
    socket.close()
    context.term()
    
    # Calcular estadísticas
    if tiempos_respuesta:
        import statistics
        promedio = statistics.mean(tiempos_respuesta)
        desv_std = statistics.stdev(tiempos_respuesta) if len(tiempos_respuesta) > 1 else 0
        minimo = min(tiempos_respuesta)
        maximo = max(tiempos_respuesta)
        
        # Calcular throughput (solicitudes por segundo)
        throughput = solicitudes_exitosas / tiempo_total if tiempo_total > 0 else 0
        
        # Mostrar resultados
        print("\n" + "=" * 70)
        print(f" RESULTADOS [{nombre_ps}]")
        print("=" * 70)
        print(f"  Duración real: {tiempo_total:.2f}s")
        print(f" Solicitudes enviadas: {solicitudes_enviadas}")
        print(f" Exitosas: {solicitudes_exitosas}")
        print(f" Fallidas: {solicitudes_fallidas}")
        print(f"\n MÉTRICAS DE RENDIMIENTO:")
        print(f"   Tiempo promedio de respuesta: {promedio:.2f} ms")
        print(f"   Desviación estándar: {desv_std:.2f} ms")
        print(f"   Tiempo mínimo: {minimo:.2f} ms")
        print(f"   Tiempo máximo: {maximo:.2f} ms")
        print(f"   Throughput: {throughput:.2f} solicitudes/segundo")
        print(f"   Solicitudes procesadas en 2min: {solicitudes_exitosas}")
        print("=" * 70)
        
        # Guardar resultados en archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archivo_resultados = f"resultado_{nombre_ps}_{timestamp}.txt"
        
        with open(archivo_resultados, 'w') as f:
            f.write(f"NOMBRE_PS={nombre_ps}\n")
            f.write(f"DURACION={tiempo_total:.2f}\n")
            f.write(f"SOLICITUDES_ENVIADAS={solicitudes_enviadas}\n")
            f.write(f"SOLICITUDES_EXITOSAS={solicitudes_exitosas}\n")
            f.write(f"SOLICITUDES_FALLIDAS={solicitudes_fallidas}\n")
            f.write(f"TIEMPO_PROMEDIO={promedio:.2f}\n")
            f.write(f"DESVIACION_ESTANDAR={desv_std:.2f}\n")
            f.write(f"TIEMPO_MINIMO={minimo:.2f}\n")
            f.write(f"TIEMPO_MAXIMO={maximo:.2f}\n")
            f.write(f"THROUGHPUT={throughput:.2f}\n")
            f.write(f"PROCESADAS_2MIN={solicitudes_exitosas}\n")
        
        print(f" Resultados guardados en: {archivo_resultados}\n")
    else:
        print(" No se obtuvieron tiempos de respuesta")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python proceso_solicitante_medicion.py <archivo> <gc_ip> <gc_puerto> <nombre_ps> [duracion_s]")
        print("\nEjemplos:")
        print("  python proceso_solicitante_medicion.py prestamos_ps1.txt 10.43.103.177 5555 PS1_Sede1")
        print("  python proceso_solicitante_medicion.py prestamos_ps2.txt 10.43.103.177 5555 PS2_Sede1 120")
        sys.exit(1)
    
    archivo = sys.argv[1]
    gc_ip = f"tcp://{sys.argv[2]}:{sys.argv[3]}"
    nombre_ps = sys.argv[4] if len(sys.argv) > 4 else "PS"
    duracion = int(sys.argv[5]) if len(sys.argv) > 5 else 120
    
    enviar_solicitudes_con_medicion(archivo, gc_ip, nombre_ps, duracion)