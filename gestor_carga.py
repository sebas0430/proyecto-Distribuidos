import zmq
import json
import time
from datetime import datetime, timedelta

class GestorCarga:
    def __init__(self, sede, puerto_rep="5555", puerto_pub="5556", puerto_prestamo="5570"):
        """
        Gestor de Carga - Coordina las operaciones del sistema
        
        Args:
            sede: número de sede (1 o 2)
            puerto_rep: puerto para recibir solicitudes de PS (REP)
            puerto_pub: puerto para publicar mensajes a Actores (PUB)
            puerto_prestamo: puerto para comunicación síncrona con Actor Préstamo (REQ)
        """
        self.sede = sede
        self.context = zmq.Context()
        
        # Socket REP: comunicación con PS
        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.bind(f"tcp://*:{puerto_rep}")
        
        # Socket PUB: comunicación con Actores (asíncrona)
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.bind(f"tcp://*:{puerto_pub}")
        
        # Socket REQ: comunicación SÍNCRONA con Actor Préstamo
        self.socket_prestamo = self.context.socket(zmq.REQ)
        self.socket_prestamo.bind(f"tcp://*:{puerto_prestamo}")
        
        print(f"⚙️  Gestor de Carga Sede {sede} iniciado")
        print(f"📡 REP (PS): puerto {puerto_rep}")
        print(f"📢 PUB (Actores Async): puerto {puerto_pub}")
        print(f"🔗 REQ (Actor Préstamo): puerto {puerto_prestamo}\n")
        
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
        """Procesa préstamo de forma SÍNCRONA a través de Actor"""
        print(f"📥 PRÉSTAMO | Usuario: {usuario} | Libro: {libro}")
        
        try:
            # Enviar solicitud al Actor Préstamo
            solicitud = {
                "operacion": "prestamo",
                "codigo": libro,
                "usuario": usuario
            }
            
            self.socket_prestamo.send_string(json.dumps(solicitud))
            
            # Esperar respuesta del Actor (síncrono)
            respuesta_json = self.socket_prestamo.recv_string()
            resultado = json.loads(respuesta_json)
            
            if resultado["exito"]:
                print(f"✅ Préstamo otorgado hasta {resultado.get('fecha_devolucion', 'N/A')}\n")
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

                 # ------------------------------------------------------------
                 # Health-check desde el monitor GC
                 # ------------------------------------------------------------
                if mensaje == "health_check":
                    self.socket_rep.send_string(json.dumps({"status": "ok"}))
                    continue
                
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
                try:
                    self.socket_rep.send_string(json.dumps(respuesta))
                except:
                    pass

if __name__ == "__main__":
    import sys
    
    # Configurar según sede
    if len(sys.argv) > 1:
        sede = int(sys.argv[1])
    else:
        print("Uso: python gestor_carga.py <sede>")
        print("Ejemplo: python gestor_carga.py 1")
        sys.exit(1)
    
    # Configuración por sede
    configuraciones = {
        1: {
            "puerto_rep": "5555",
            "puerto_pub": "5556",
            "puerto_prestamo": "5570"
        },
        2: {
            "puerto_rep": "5565",
            "puerto_pub": "5566",
            "puerto_prestamo": "5571"
        }
    }
    
    config = configuraciones.get(sede)
    if not config:
        print(f"❌ Sede {sede} no válida. Use 1 o 2")
        sys.exit(1)
    
    gc = GestorCarga(
        sede=sede,
        puerto_rep=config["puerto_rep"],
        puerto_pub=config["puerto_pub"],
        puerto_prestamo=config["puerto_prestamo"]
    )
    
    gc.ejecutar()