import zmq
import json
import sys
import time

class Actor:
    def __init__(self, tipo_actor, gc_pub_port="5556", ga_req_port="5557", gc_ip="tcp://10.43.103.177"):
        """
        Actor que procesa operaciones del sistema
        
        Args:
            tipo_actor: "devolucion", "renovacion" o "prestamo"
            gc_pub_port: puerto PUB del Gestor de Carga
            ga_req_port: puerto REP del Gestor de Almacenamiento
            gc_ip: IP del Gestor de Carga
        """
        self.tipo = tipo_actor
        self.context = zmq.Context()
        
        if tipo_actor in ["devolucion", "renovacion"]:
            # Patrón PUB-SUB para operaciones asíncronas
            self.socket_sub = self.context.socket(zmq.SUB)
            self.socket_sub.connect(f"{gc_ip}:{gc_pub_port}")
            self.socket_sub.setsockopt(zmq.SUBSCRIBE, tipo_actor.encode())
            print(f"🎭 Actor {tipo_actor.upper()} suscrito al canal '{tipo_actor}'")
        
        # Socket para comunicarse con GA
        self.socket_ga = self.context.socket(zmq.REQ)
        self.socket_ga.connect(f"{gc_ip}:{ga_req_port}")
        
        print(f"💾 Conectado al GA en puerto {ga_req_port}\n")
    
    def procesar_devolucion_async(self):
        """Procesa devoluciones (modo asíncrono vía PUB-SUB)"""
        print("📡 Esperando devoluciones...\n")
        
        while True:
            try:
                # Recibir del canal: "devolucion usuario,libro"
                mensaje = self.socket_sub.recv_string()
                topico, contenido = mensaje.split(" ", 1)
                usuario, libro = contenido.split(",", 1)
                
                print(f"📥 DEVOLUCIÓN | Usuario: {usuario} | Libro: {libro}")
                
                # Enviar a GA para actualizar BD
                solicitud = {
                    "operacion": "devolucion",
                    "codigo": libro.strip(),
                    "usuario": usuario.strip()
                }
                
                self.socket_ga.send_string(json.dumps(solicitud))
                respuesta_json = self.socket_ga.recv_string()
                respuesta = json.loads(respuesta_json)
                
                if respuesta["exito"]:
                    print(f"✅ {respuesta['mensaje']}\n")
                else:
                    print(f"❌ {respuesta['mensaje']}\n")
                
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                print("\n🛑 Deteniendo Actor de Devoluciones...")
                break
            except Exception as e:
                print(f"❌ Error procesando devolución: {e}\n")
    
    def procesar_renovacion_async(self):
        """Procesa renovaciones (modo asíncrono vía PUB-SUB)"""
        print("📡 Esperando renovaciones...\n")
        
        while True:
            try:
                # Recibir del canal: "renovacion usuario,libro"
                mensaje = self.socket_sub.recv_string()
                topico, contenido = mensaje.split(" ", 1)
                usuario, libro = contenido.split(",", 1)
                
                print(f"📥 RENOVACIÓN | Usuario: {usuario} | Libro: {libro}")
                
                # Enviar a GA para actualizar BD
                solicitud = {
                    "operacion": "renovacion",
                    "codigo": libro.strip(),
                    "usuario": usuario.strip()
                }
                
                self.socket_ga.send_string(json.dumps(solicitud))
                respuesta_json = self.socket_ga.recv_string()
                respuesta = json.loads(respuesta_json)
                
                if respuesta["exito"]:
                    print(f"✅ {respuesta['mensaje']} - Nueva fecha: {respuesta.get('nueva_fecha', 'N/A')}\n")
                else:
                    print(f"❌ {respuesta['mensaje']}\n")
                
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                print("\n🛑 Deteniendo Actor de Renovaciones...")
                break
            except Exception as e:
                print(f"❌ Error procesando renovación: {e}\n")
    
    def ejecutar(self):
        """Inicia el procesamiento según el tipo de actor"""
        if self.tipo == "devolucion":
            self.procesar_devolucion_async()
        elif self.tipo == "renovacion":
            self.procesar_renovacion_async()
        else:
            print(f"❌ Tipo de actor desconocido: {self.tipo}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python actor_completo.py <tipo>")
        print("Tipos: devolucion, renovacion")
        sys.exit(1)
    
    tipo_actor = sys.argv[1].lower()
    
    # Configuración (ajustar según tu red)
    GC_IP = "tcp://10.43.103.177"  # Cambiar por IP real del GC
    GC_PUB_PORT = "5556"
    GA_REQ_PORT = "5557"
    
    actor = Actor(
        tipo_actor=tipo_actor,
        gc_pub_port=GC_PUB_PORT,
        ga_req_port=GA_REQ_PORT,
        gc_ip=GC_IP
    )
    
    actor.ejecutar()