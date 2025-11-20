import zmq
import json
import time
import sys

class MonitorGC:
    def __init__(self, gc_primario_ip, gc_primario_port, gc_replica_ip, gc_replica_port, sede):
        """
        Monitor del Gestor de Carga (GC)
        Detecta fallas del GC primario y activa al GC réplica.
        """

        self.gc_primario = f"tcp://{gc_primario_ip}:{gc_primario_port}"
        self.gc_replica = f"tcp://{gc_replica_ip}:{gc_replica_port}"

        self.gc_activo = self.gc_primario
        self.sede = sede

        self.context = zmq.Context()

        # Contador de fallas consecutivas
        self.intentos = 0
        self.MAX_INTENTOS = 3

        # Socket PUB para notificar a actores, PS, etc.
        self.pub = self.context.socket(zmq.PUB)
        self.pub.bind("tcp://*:6001")  # canal de notificaciones GC

        print(f" Monitor GC Sede {sede} iniciado")
        print(f"GC Primario: {self.gc_primario}")
        print(f" GC Réplica : {self.gc_replica}\n")


    # -------------------------------------------------------------
    # HEALTH-CHECK
    # -------------------------------------------------------------
    def health(self, endpoint):
        """
        Verifica que el GC responda correctamente al health_check.
        """
        try:
            sock = self.context.socket(zmq.REQ)
            sock.setsockopt(zmq.RCVTIMEO, 2000)   # timeout de 2 sec
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(endpoint)

            sock.send_string("health_check")
            resp = json.loads(sock.recv_string())

            sock.close()

            return resp.get("status") == "ok"

        except:
            return False


    # -------------------------------------------------------------
    # FAILOVER AUTOMÁTICO
    # -------------------------------------------------------------
    def activar_replica(self):
        print("\n" + "="*60)
        print(" FALLA DETECTADA EN GC PRIMARIO")
        print("="*60)

        print(f" Activando GC réplica: {self.gc_replica}")
        self.gc_activo = self.gc_replica

        # Notificación para TODOS los componentes del sistema
        msg = {
            "evento": "gc_failover",
            "nuevo_endpoint": self.gc_replica
        }

        self.pub.send_string(json.dumps(msg))

        print(" Notificación enviada al sistema (gc_failover)")
        print("="*60 + "\n")


    # -------------------------------------------------------------
    # LOOP PRINCIPAL DEL MONITOR
    # -------------------------------------------------------------
    def monitorear(self):
        print(" Iniciando monitoreo continuo del GC...\n")

        while True:
            try:
                esta_vivo = self.health(self.gc_activo)

                if esta_vivo:
                    print(f"[{time.strftime('%H:%M:%S')}] GC OK → {self.gc_activo}")
                    self.intentos = 0
                else:
                    self.intentos += 1
                    print(f"[{time.strftime('%H:%M:%S')}]  GC no responde "
                          f"({self.intentos}/{self.MAX_INTENTOS})")

                    if self.intentos >= self.MAX_INTENTOS:
                        # Si es el primario → activar réplica
                        if self.gc_activo == self.gc_primario:
                            self.activar_replica()
                        else:
                            print(" La réplica del GC también falló. Sistema crítico.")

                        self.intentos = 0

                # Tiempo entre health-checks
                time.sleep(5)

            except KeyboardInterrupt:
                print("\n Monitor GC detenido por el usuario.")
                break

            except Exception as e:
                print(f" Error en el monitor: {e}")
                time.sleep(1)


# ==============================================================
# CONFIGURACIÓN POR SEDE 
# ==============================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python monitor_gc.py <sede>")
        sys.exit(1)

    sede = int(sys.argv[1])

 
    # Sede 1 → 10.43.103.177
    # Sede 2 → 10.43.103.132

    if sede == 1:
        monitor = MonitorGC(
            gc_primario_ip="10.43.103.177",  # GC sede 1
            gc_primario_port="5555",
            gc_replica_ip="10.43.103.132",   # GC sede 2
            gc_replica_port="5565",
            sede=1
        )
    else:
        monitor = MonitorGC(
            gc_primario_ip="10.43.103.132",  # GC sede 2
            gc_primario_port="5565",
            gc_replica_ip="10.43.103.177",   # GC sede 1
            gc_replica_port="5555",
            sede=2
        )

    monitor.monitorear()