import zmq
import json
import sys
import time

class ActorPrestamo:
    def __init__(self, gc_ip, gc_prestamo_port, ga_req_port):
        """
        Actor que procesa operaciones de PRÉSTAMO de forma SÍNCRONA
        
        Args:
            gc_ip: IP del Gestor de Carga (formato: tcp://10.43.103.177)
            gc_prestamo_port: puerto donde GC envía solicitudes de préstamo
            ga_req_port: puerto del Gestor de Almacenamiento
        """
        self.context = zmq.Context()
        
        # Socket REP: recibe solicitudes de préstamo del GC
        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.connect(f"{gc_ip}:{gc_prestamo_port}")
        
        # Socket REQ: comunica con GA
        self.socket_ga = self.context.socket(zmq.REQ)
        self.socket_ga.connect(f"{gc_ip}:{ga_req_port}")
        
        print(f"🎭 Actor PRÉSTAMO iniciado")
        print(f"📡 Conectado al GC en {gc_ip}:{gc_prestamo_port}")
        print(f"💾 Conectado al GA en {gc_ip}:{ga_req_port}\n")
    
    def procesar_prestamos(self):
        """Procesa solicitudes de préstamo de forma síncrona"""
        print("📡 Esperando solicitudes de préstamo...\n")
        
        while True:
            try:
                # 1. Recibir solicitud del GC
                mensaje = self.socket_rep.recv_string()
                solicitud = json.loads(mensaje)
                
                print(f"📥 PRÉSTAMO SÍNCRONO | Usuario: {solicitud['usuario']} | Libro: {solicitud['codigo']}")
                
                # 2. Verificar disponibilidad en GA
                verificacion = {
                    "operacion": "verificar_disponibilidad",
                    "codigo": solicitud["codigo"]
                }
                
                self.socket_ga.send_string(json.dumps(verificacion))
                respuesta_verificacion = json.loads(self.socket_ga.recv_string())
                
                if not respuesta_verificacion.get("disponible", False):
                    # Libro no disponible
                    print(f"❌ {respuesta_verificacion['mensaje']}\n")
                    self.socket_rep.send_string(json.dumps({
                        "exito": False,
                        "mensaje": respuesta_verificacion["mensaje"]
                    }))
                    continue
                
                # 3. Realizar préstamo en GA
                prestamo_solicitud = {
                    "operacion": "prestamo",
                    "codigo": solicitud["codigo"],
                    "usuario": solicitud["usuario"]
                }
                
                self.socket_ga.send_string(json.dumps(prestamo_solicitud))
                respuesta_prestamo = json.loads(self.socket_ga.recv_string())
                
                # 4. Responder al GC
                self.socket_rep.send_string(json.dumps(respuesta_prestamo))
                
                if respuesta_prestamo.get("exito", False):
                    print(f"✅ {respuesta_prestamo['mensaje']} - Fecha devolución: {respuesta_prestamo.get('fecha_devolucion', 'N/A')}\n")
                else:
                    print(f"❌ {respuesta_prestamo['mensaje']}\n")
                
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                print("\n🛑 Deteniendo Actor de Préstamos...")
                break
            except Exception as e:
                print(f"❌ Error procesando préstamo: {e}")
                try:
                    self.socket_rep.send_string(json.dumps({
                        "exito": False,
                        "mensaje": f"Error del sistema: {str(e)}"
                    }))
                except:
                    pass

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python actor_prestamo.py <gc_ip> <gc_prestamo_port> <ga_req_port>")
        print("\nEjemplos:")
        print("  Sede 1: python actor_prestamo.py tcp://10.43.103.177 5570 5557")
        print("  Sede 2: python actor_prestamo.py tcp://10.43.103.132 5571 5558")
        sys.exit(1)
    
    GC_IP = sys.argv[1]
    GC_PRESTAMO_PORT = sys.argv[2]
    GA_REQ_PORT = sys.argv[3]
    
    actor = ActorPrestamo(GC_IP, GC_PRESTAMO_PORT, GA_REQ_PORT)
    actor.procesar_prestamos()