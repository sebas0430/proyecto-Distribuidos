import zmq
import json
import time
from datetime import datetime, timedelta

class GestorCarga:
    def __init__(self, sede, puerto_rep="5555", puerto_pub="5556", ga_port="5557"):
        """
        Gestor de Carga - Coordina las operaciones del sistema
        
        Args:
            sede: número de sede (1 o 2)
            puerto_rep: puerto para recibir solicitudes de PS (REP)
            puerto_pub: puerto para publicar mensajes a Actores (PUB)
            ga_port: puerto del Gestor de Almacenamiento
        """
        self.sede = sede
        self.context = zmq.Context()
        
        # Socket REP: comunicación con PS
        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.bind(f"tcp://*:{puerto_rep}")
        
        # Socket PUB: comunicación con Actores (asíncrona)
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.bind(f"tcp://*:{puerto_pub}")
        
        # Socket REQ: comunicación con GA (síncrona para préstamos)
        self.socket_ga = self.context.socket(zmq.REQ)
        self.socket_ga.connect(f"tcp://localhost:{ga_port}")
        
        print(f"⚙️  Gestor de Carga Sede {sede} iniciado")
        print(f"📡 REP (PS): puerto {puerto_rep}")
        print(f"📢 PUB (Actores): puerto {puerto_pub}")
        print(f"💾 REQ (GA): puerto {ga_port}\n")
        
        # Pequeña pausa para que PUB se establezca
        time.sleep(0.5)
    
    def procesar_devolucion(self, usuario, libro):
        """Procesa devolución de forma asíncrona"""
        print(f"📥 DEVOLUCIÓN | Usuario: {usuario} | Libro: {libro}")
        
        # Respuesta inmediata al PS
        respuesta = {
            "exito": True,
            "mensaje": "Biblioteca recibiendo el libro. Gracias por devolverlo."
        }
        
        # Publicar al canal para que Actor lo procese
        self.socket_pub.send_string(f"devolucion {usuario},{libro}")
        print(f"📢 Publicado en canal 'devolucion'\n")
        
        return respuesta
    
    def procesar_renovacion(self, usuario, libro):
        """Procesa renovación de forma asíncrona"""
        print(f"📥 RENOVACIÓN | Usuario: {usuario} | Libro: {libro}")
        
        # Calcular nueva fecha (1 semana adicional)
        fecha_actual = datetime.now()
        nueva_fecha = fecha_actual + timedelta(weeks=1)
        
        # Respuesta inmediata al PS
        respuesta = {
            "exito": True,
            "mensaje": f"Renovación aceptada. Nueva fecha de entrega: {nueva_fecha.strftime('%Y-%m-%d')}"
        }
        
        # Publicar al canal para que Actor lo procese
        self.socket_pub.send_string(f"renovacion {usuario},{libro}")
        print(f"📢 Publicado en canal 'renovacion'\n")
        
        return respuesta
    
    def procesar_prestamo(self, usuario, libro):
        """Procesa préstamo de forma SÍNCRONA"""
        print(f"📥 PRÉSTAMO | Usuario: {usuario} | Libro: {libro}")
        
        try:
            # 1. Verificar disponibilidad en GA
            solicitud = {
                "operacion": "verificar_disponibilidad",
                "codigo": libro
            }
            
            self.socket_ga.send_string(json.dumps(solicitud))
            respuesta_json = self.socket_ga.recv_string()
            verificacion = json.loads(respuesta_json)
            
            if not verificacion["disponible"]:
                print(f"❌ {verificacion['mensaje']}\n")
                return {
                    "exito": False,
                    "mensaje": verificacion["mensaje"]
                }
            
            # 2. Realizar préstamo en GA
            solicitud = {
                "operacion": "prestamo",
                "codigo": libro,
                "usuario": usuario
            }
            
            self.socket_ga.send_string(json.dumps(solicitud))
            respuesta_json = self.socket_ga.recv_string()
            resultado = json.loads(respuesta_json)
            
            if resultado["exito"]:
                print(f"✅ Préstamo otorgado hasta {resultado['fecha_devolucion']}\n")
            else:
                print(f"❌ {resultado['mensaje']}\n")
            
            return resultado
            
        except Exception as e:
            print(f"❌ Error procesando préstamo: {e}\n")
            return {
                "exito": False,
                "mensaje": f"Error del sistema: {str(e)}"
            }
    
    def ejecutar(self):
        """Loop principal del GC"""
        print("🚀 Gestor de Carga listo para recibir solicitudes...\n")
        
        while True:
            try:
                # Recibir solicitud del PS
                mensaje = self.socket_rep.recv_string()
                print(f"📩 Mensaje recibido: {mensaje}")
                
                # Parsear mensaje: "tipo,usuario,libro"
                try:
                    tipo, usuario, libro = mensaje.split(",")
                    tipo = tipo.strip().lower()
                    usuario = usuario.strip()
                    libro = libro.strip()
                except ValueError:
                    respuesta = {
                        "exito": False,
                        "mensaje": "Formato de mensaje inválido. Use: tipo,usuario,libro"
                    }
                    self.socket_rep.send_string(json.dumps(respuesta))
                    continue
                
                # Procesar según tipo
                if tipo == "devolucion":
                    respuesta = self.procesar_devolucion(usuario, libro)
                elif tipo == "renovacion":
                    respuesta = self.procesar_renovacion(usuario, libro)
                elif tipo == "prestamo":
                    respuesta = self.procesar_prestamo(usuario, libro)
                else:
                    respuesta = {
                        "exito": False,
                        "mensaje": f"Tipo de operación desconocido: {tipo}"
                    }
                    print(f"❌ Tipo desconocido: {tipo}\n")
                
                # Enviar respuesta al PS
                self.socket_rep.send_string(json.dumps(respuesta))
                
            except KeyboardInterrupt:
                print("\n🛑 Deteniendo Gestor de Carga...")
                break
            except Exception as e:
                print(f"❌ Error general: {e}")
                respuesta = {"exito": False, "mensaje": str(e)}
                self.socket_rep.send_string(json.dumps(respuesta))

if __name__ == "__main__":
    import sys
    
    # Configurar según sede
    if len(sys.argv) > 1:
        sede = int(sys.argv[1])
    else:
        sede = 1
    
    # Configuración por sede
    configuraciones = {
        1: {
            "puerto_rep": "5555",
            "puerto_pub": "5556",
            "ga_port": "5557"
        },
        2: {
            "puerto_rep": "5565",
            "puerto_pub": "5566",
            "ga_port": "5558"
        }
    }
    
    config = configuraciones[sede]
    gc = GestorCarga(
        sede=sede,
        puerto_rep=config["puerto_rep"],
        puerto_pub=config["puerto_pub"],
        ga_port=config["ga_port"]
    )
    
    gc.ejecutar()