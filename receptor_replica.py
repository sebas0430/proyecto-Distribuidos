import zmq
import json
import sqlite3
import sys
import os

class ReceptorReplica:
    def __init__(self, sede, puerto_pull="5559"):
        """
        Receptor que actualiza la réplica secundaria de forma asíncrona
        
        Args:
            sede: número de sede (1 o 2)
            puerto_pull: puerto para recibir actualizaciones (PULL)
        """
        self.sede = sede
        self.db_file = f"bd_sede{sede}_replica.db"
        
        self.context = zmq.Context()
        
        # Socket PULL: recibe actualizaciones de la sede primaria
        self.socket_pull = self.context.socket(zmq.PULL)
        self.socket_pull.bind(f"tcp://*:{puerto_pull}")
        
        print(f" Receptor de Réplica Sede {sede} iniciado")
        print(f" PULL: puerto {puerto_pull}")
        print(f" BD Réplica SQLite: {self.db_file}\n")
        
        # Inicializar BD réplica si no existe
        self.inicializar_bd_replica()
    
    def get_connection(self):
        """Crea una conexión a la BD réplica"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn
    
    def inicializar_bd_replica(self):
        """Crea las tablas de la réplica si no existen"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Crear tabla libros
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS libros (
                codigo TEXT PRIMARY KEY,
                titulo TEXT NOT NULL,
                autor TEXT NOT NULL,
                ejemplares_totales INTEGER NOT NULL,
                ejemplares_disponibles INTEGER NOT NULL
            )
        ''')
        
        # Crear tabla prestamos
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS prestamos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT NOT NULL,
                usuario TEXT NOT NULL,
                fecha_prestamo TEXT NOT NULL,
                fecha_devolucion TEXT NOT NULL,
                renovaciones INTEGER DEFAULT 0,
                FOREIGN KEY (codigo) REFERENCES libros(codigo)
            )
        ''')
        
        cursor.execute("SELECT COUNT(*) FROM libros")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"✅ BD réplica cargada: {count} libros")
        else:
            print("⚠️  BD réplica vacía. Se sincronizará con las operaciones.")
        
        conn.commit()
        conn.close()
    
    def aplicar_prestamo(self, operacion):
        """Aplica un préstamo en la réplica"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            codigo = operacion["codigo"]
            usuario = operacion["usuario"]
            fecha_prestamo = operacion["fecha_prestamo"]
            fecha_devolucion = operacion["fecha_devolucion"]
            
            # Insertar préstamo
            cursor.execute(
                "INSERT INTO prestamos (codigo, usuario, fecha_prestamo, fecha_devolucion, renovaciones) VALUES (?, ?, ?, ?, ?)",
                (codigo, usuario, fecha_prestamo, fecha_devolucion, 0)
            )
            
            # Reducir disponibilidad
            cursor.execute(
                "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles - 1 WHERE codigo = ?",
                (codigo,)
            )
            
            conn.commit()
            print(f"✅ REPLICADO: Préstamo de {codigo} a {usuario}")
        
        except Exception as e:
            conn.rollback()
            print(f"❌ Error replicando préstamo: {e}")
        finally:
            conn.close()
    
    def aplicar_devolucion(self, operacion):
        """Aplica una devolución en la réplica"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            codigo = operacion["codigo"]
            usuario = operacion["usuario"]
            
            # Eliminar préstamo
            cursor.execute(
                "DELETE FROM prestamos WHERE codigo = ? AND usuario = ?",
                (codigo, usuario)
            )
            
            # Aumentar disponibilidad
            cursor.execute(
                "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles + 1 WHERE codigo = ?",
                (codigo,)
            )
            
            conn.commit()
            print(f"✅ REPLICADO: Devolución de {codigo} por {usuario}")
        
        except Exception as e:
            conn.rollback()
            print(f"❌ Error replicando devolución: {e}")
        finally:
            conn.close()
    
    def aplicar_renovacion(self, operacion):
        """Aplica una renovación en la réplica"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            codigo = operacion["codigo"]
            usuario = operacion["usuario"]
            nueva_fecha = operacion["nueva_fecha"]
            renovaciones = operacion["renovaciones"]
            
            # Actualizar préstamo
            cursor.execute(
                "UPDATE prestamos SET fecha_devolucion = ?, renovaciones = ? WHERE codigo = ? AND usuario = ?",
                (nueva_fecha, renovaciones, codigo, usuario)
            )
            
            conn.commit()
            print(f"✅ REPLICADO: Renovación {renovaciones}/2 de {codigo} por {usuario}")
        
        except Exception as e:
            conn.rollback()
            print(f"❌ Error replicando renovación: {e}")
        finally:
            conn.close()
    
    def aplicar_operacion(self, operacion):
        """Aplica una operación en la BD réplica"""
        tipo = operacion.get("tipo")
        
        if tipo == "prestamo":
            self.aplicar_prestamo(operacion)
        elif tipo == "devolucion":
            self.aplicar_devolucion(operacion)
        elif tipo == "renovacion":
            self.aplicar_renovacion(operacion)
        else:
            print(f"⚠️  Operación desconocida: {tipo}")
    
    def ejecutar(self):
        """Loop principal del receptor"""
        print(" Esperando actualizaciones de la BD primaria...\n")
        
        while True:
            try:
                # Recibir operación de replicación
                mensaje = self.socket_pull.recv_string()
                operacion = json.loads(mensaje)
                
                print(f" Operación recibida: {operacion.get('tipo', 'desconocido')}")
                
                # Aplicar operación en réplica
                self.aplicar_operacion(operacion)
                
            except KeyboardInterrupt:
                print("\n Deteniendo Receptor de Réplica...")
                break
            except Exception as e:
                print(f"❌ Error procesando replicación: {e}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python receptor_replica.py <sede>")
        print("Ejemplo: python receptor_replica.py 1")
        sys.exit(1)
    
    sede = int(sys.argv[1])
    
    # Configuración por sede
    puerto_pull = "5559" if sede == 1 else "5560"
    
    receptor = ReceptorReplica(sede=sede, puerto_pull=puerto_pull)
    receptor.ejecutar()
