import zmq
import json
import time
import subprocess
import sys

class MonitorGA:
    def __init__(self, ga_primario_ip, ga_primario_port, ga_replica_ip, ga_replica_port, sede):
        """
        Monitor que detecta fallas del GA y activa réplica
        
        Args:
            ga_primario_ip: IP del GA primario
            ga_primario_port: puerto del GA primario
            ga_replica_ip: IP del GA réplica
            ga_replica_port: puerto del GA réplica
            sede: número de sede
        """
        self.ga_primario = f"tcp://{ga_primario_ip}:{ga_primario_port}"
        self.ga_replica = f"tcp://{ga_replica_ip}:{ga_replica_port}"
        self.sede = sede
        self.ga_activo = self.ga_primario
        self.context = zmq.Context()
        self.intentos_fallo = 0
        self.MAX_INTENTOS = 3
        
        print(f"Monitor GA Sede {sede} iniciado")
        print(f"GA Primario: {self.ga_primario}")
        print(f"GA Réplica: {self.ga_replica}\n")
    
    def verificar_salud(self, ga_endpoint):
        """Envía ping al GA para verificar que responde"""
        try:
            socket = self.context.socket(zmq.REQ)
            socket.setsockopt(zmq.RCVTIMEO, 2000)  # Timeout 2 segundos
            socket.setsockopt(zmq.LINGER, 0)
            socket.connect(ga_endpoint)
            
            # Enviar health check
            solicitud = {
                "operacion": "health_check"
            }
            
            socket.send_string(json.dumps(solicitud))
            respuesta_json = socket.recv_string()
            respuesta = json.loads(respuesta_json)
            
            socket.close()
            
            return respuesta.get("status") == "ok"
            
        except zmq.error.Again:
            # Timeout - GA no responde
            return False
        except Exception as e:
            print(f" Error verificando salud: {e}")
            return False
    
    def activar_replica(self):
        """Activa la réplica como GA primario"""
        print("\n" + "="*60)
        print("FALLA DETECTADA EN GA PRIMARIO")
        print("="*60)
        print(f"Activando réplica como GA primario...")
        print(f"Nuevo GA activo: {self.ga_replica}")
        
        self.ga_activo = self.ga_replica
        
        # Aquí se deberían notificar todos los componentes
        # Para simplificar, los componentes deberían intentar reconectar automáticamente
        
        print(" Réplica activada exitosamente")
        print("="*60 + "\n")
    
    def monitorear(self):
        """Loop principal de monitoreo"""
        print(" Iniciando monitoreo continuo del GA...\n")
        
        while True:
            try:
                # Verificar salud del GA activo
                esta_saludable = self.verificar_salud(self.ga_activo)
                
                if esta_saludable:
                    print(f"[{time.strftime('%H:%M:%S')}] GA respondiendo correctamente")
                    self.intentos_fallo = 0
                else:
                    self.intentos_fallo += 1
                    print(f"  [{time.strftime('%H:%M:%S')}] GA no responde (intento {self.intentos_fallo}/{self.MAX_INTENTOS})")
                    
                    if self.intentos_fallo >= self.MAX_INTENTOS:
                        # Si es el primario, activar réplica
                        if self.ga_activo == self.ga_primario:
                            self.activar_replica()
                            self.intentos_fallo = 0
                        else:
                            print(" Réplica también falló. Sistema crítico.")
                
                # Esperar antes del próximo check
                time.sleep(5)
                
            except KeyboardInterrupt:
                print("\nDeteniendo monitor...")
                break
            except Exception as e:
                print(f" Error en monitoreo: {e}")
                time.sleep(5)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python monitor_ga.py <sede>")
        print("Ejemplo: python monitor_ga.py 1")
        sys.exit(1)
    
    sede = int(sys.argv[1])
    
    # Configuración
    if sede == 1:
        monitor = MonitorGA(
            ga_primario_ip="10.43.103.177",
            ga_primario_port="5557",
            ga_replica_ip="10.43.103.132",
            ga_replica_port="5558",
            sede=sede
        )
    else:
        monitor = MonitorGA(
            ga_primario_ip="10.43.103.132",
            ga_primario_port="5558",
            ga_replica_ip="10.43.103.177",
            ga_replica_port="5557",
            sede=sede
        )
    
    monitor.monitorear()