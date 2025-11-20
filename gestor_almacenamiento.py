import zmq
import json
import sqlite3
import time
from datetime import datetime, timedelta
import sys
import os

class GestorAlmacenamiento:
    def __init__(self, sede, puerto_rep="5557", replica_ip=None, replica_port=None):
        """
        Gestor de Almacenamiento - Maneja BD SQLite primaria y r�plica
        
        Args:
            sede: n�mero de sede (1 o 2)
            puerto_rep: puerto para recibir solicitudes (REP)
            replica_ip: IP de la r�plica secundaria
            replica_port: puerto de la r�plica secundaria
        """
        self.sede = sede
        self.db_file = f"bd_sede{sede}.db"
        self.replica_ip = replica_ip
        self.replica_port = replica_port
        
        self.context = zmq.Context()
        
        # Socket REP: recibe solicitudes de Actores y GC
        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.bind(f"tcp://*:{puerto_rep}")
        
        # Socket PUSH: para comunicarse con r�plica (as�ncrono)
        self.socket_replica = None
        if replica_ip and replica_port:
            self.socket_replica = self.context.socket(zmq.PUSH)
            self.socket_replica.connect(f"tcp://{replica_ip}:{replica_port}")
            print(f"= Conectado a r�plica en {replica_ip}:{replica_port}")
            time.sleep(1)  # Esperar a que PULL est� listo
        
        print(f"=� Gestor de Almacenamiento Sede {sede} iniciado")
        print(f"=� REP: puerto {puerto_rep}")
        print(f"=� Base de datos SQLite: {self.db_file}\n")
        
        # Inicializar BD
        self.inicializar_bd()
        
    def get_connection(self):
        """Crea una conexi�n a la BD SQLite"""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row  # Para acceder por nombre de columna
        return conn
    
    def inicializar_bd(self):
        """Crea las tablas si no existen e inserta datos iniciales"""
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
        
        # Verificar si ya hay datos
        cursor.execute("SELECT COUNT(*) FROM libros")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("�  Inicializando BD con 1000 libros...")
            
            # Insertar 1000 libros
            libros = []
            for i in range(1, 1001):
                codigo = f"ISBN{i:04d}"
                titulo = f"Libro {i}"
                autor = f"Autor {i % 100}"
                ejemplares_totales = 1 if i % 10 == 0 else (i % 5 + 1)
                ejemplares_disponibles = ejemplares_totales
                
                libros.append((codigo, titulo, autor, ejemplares_totales, ejemplares_disponibles))
            
            cursor.executemany(
                "INSERT INTO libros VALUES (?, ?, ?, ?, ?)",
                libros
            )
            
            # Crear pr�stamos iniciales (50 sede 1, 150 sede 2)
            prestamos_por_sede = 50 if self.sede == 1 else 150
            fecha_prestamo = datetime.now().strftime("%Y-%m-%d")
            fecha_devolucion = (datetime.now() + timedelta(weeks=2)).strftime("%Y-%m-%d")
            
            prestamos = []
            for i in range(prestamos_por_sede):
                codigo = f"ISBN{i+1:04d}"
                usuario = f"user{i+1}"
                prestamos.append((codigo, usuario, fecha_prestamo, fecha_devolucion, 0))
            
            cursor.executemany(
                "INSERT INTO prestamos (codigo, usuario, fecha_prestamo, fecha_devolucion, renovaciones) VALUES (?, ?, ?, ?, ?)",
                prestamos
            )
            
            # Actualizar disponibilidad de libros prestados
            for codigo, _, _, _, _ in prestamos:
                cursor.execute(
                    "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles - 1 WHERE codigo = ?",
                    (codigo,)
                )
            
            conn.commit()
            print(f" BD inicializada: 1000 libros, {prestamos_por_sede} pr�stamos")
        else:
            print(f" BD cargada: {count} libros existentes")
        
        conn.close()
    
    def replicar_operacion(self, operacion):
        """Env�a operaci�n a r�plica de forma as�ncrona"""
        if self.socket_replica:
            try:
                mensaje = json.dumps(operacion)
                print(f"= Intentando replicar a {self.replica_ip}:{self.replica_port}")
                print(f"   Operaci�n: {operacion.get('tipo', 'desconocido')} - C�digo: {operacion.get('codigo', 'N/A')} - Usuario: {operacion.get('usuario', 'N/A')}")
                self.socket_replica.send_string(mensaje, zmq.NOBLOCK)
                print(f" Operaci�n enviada correctamente a la r�plica\n")
            except zmq.error.Again:
                print(f"�  R�plica ocupada, operaci�n no replicada\n")
            except Exception as e:
                print(f"L Error replicando: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"�  Socket de r�plica no inicializado - NO SE REPLICA\n")
    
    def verificar_disponibilidad(self, codigo):
        """Verifica si hay ejemplares disponibles de un libro"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM libros WHERE codigo = ?", (codigo,))
        libro = cursor.fetchone()
        conn.close()
        
        if not libro:
            return {
                "disponible": False,
                "mensaje": f"El libro {codigo} no existe en la biblioteca"
            }
        
        if libro['ejemplares_disponibles'] <= 0:
            return {
                "disponible": False,
                "mensaje": f"No hay ejemplares disponibles de '{libro['titulo']}'"
            }
        
        return {
            "disponible": True,
            "mensaje": f"Hay {libro['ejemplares_disponibles']} ejemplar(es) disponible(s)",
            "libro": dict(libro)
        }
    
    def realizar_prestamo(self, codigo, usuario):
        """Realiza un pr�stamo de libro"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Verificar disponibilidad
            cursor.execute("SELECT * FROM libros WHERE codigo = ?", (codigo,))
            libro = cursor.fetchone()
            
            if not libro or libro['ejemplares_disponibles'] <= 0:
                conn.close()
                return {
                    "exito": False,
                    "mensaje": "Libro no disponible"
                }
            
            # Actualizar disponibilidad
            cursor.execute(
                "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles - 1 WHERE codigo = ?",
                (codigo,)
            )
            
            # Crear pr�stamo
            fecha_prestamo = datetime.now().strftime("%Y-%m-%d")
            fecha_devolucion = (datetime.now() + timedelta(weeks=2)).strftime("%Y-%m-%d")
            
            cursor.execute(
                "INSERT INTO prestamos (codigo, usuario, fecha_prestamo, fecha_devolucion, renovaciones) VALUES (?, ?, ?, ?, ?)",
                (codigo, usuario, fecha_prestamo, fecha_devolucion, 0)
            )
            
            conn.commit()
            conn.close()
            
            # Replicar
            self.replicar_operacion({
                "tipo": "prestamo",
                "codigo": codigo,
                "usuario": usuario,
                "fecha_prestamo": fecha_prestamo,
                "fecha_devolucion": fecha_devolucion
            })
            
            return {
                "exito": True,
                "mensaje": f"Pr�stamo otorgado de '{libro['titulo']}'",
                "fecha_devolucion": fecha_devolucion
            }
        
        except Exception as e:
            conn.rollback()
            conn.close()
            return {
                "exito": False,
                "mensaje": f"Error realizando pr�stamo: {str(e)}"
            }
    
    def realizar_devolucion(self, codigo, usuario):
        """Procesa la devoluci�n de un libro"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Buscar pr�stamo
            cursor.execute(
                "SELECT * FROM prestamos WHERE codigo = ? AND usuario = ?",
                (codigo, usuario)
            )
            prestamo = cursor.fetchone()
            
            if not prestamo:
                conn.close()
                return {
                    "exito": False,
                    "mensaje": f"No se encontr� pr�stamo activo para {usuario} del libro {codigo}"
                }
            
            # Eliminar pr�stamo
            cursor.execute(
                "DELETE FROM prestamos WHERE codigo = ? AND usuario = ?",
                (codigo, usuario)
            )
            
            # Aumentar disponibilidad
            cursor.execute(
                "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles + 1 WHERE codigo = ?",
                (codigo,)
            )
            
            # Obtener t�tulo del libro
            cursor.execute("SELECT titulo, ejemplares_disponibles FROM libros WHERE codigo = ?", (codigo,))
            libro = cursor.fetchone()
            
            conn.commit()
            conn.close()
            
            # Replicar
            self.replicar_operacion({
                "tipo": "devolucion",
                "codigo": codigo,
                "usuario": usuario
            })
            
            return {
                "exito": True,
                "mensaje": f"Devoluci�n de '{libro['titulo']}' registrada. Ejemplares disponibles: {libro['ejemplares_disponibles']}"
            }
        
        except Exception as e:
            conn.rollback()
            conn.close()
            return {
                "exito": False,
                "mensaje": f"Error en devoluci�n: {str(e)}"
            }
    
    def realizar_renovacion(self, codigo, usuario):
        """Procesa la renovaci�n de un pr�stamo"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Buscar pr�stamo
            cursor.execute(
                "SELECT * FROM prestamos WHERE codigo = ? AND usuario = ?",
                (codigo, usuario)
            )
            prestamo = cursor.fetchone()
            
            if not prestamo:
                conn.close()
                return {
                    "exito": False,
                    "mensaje": f"No se encontr� pr�stamo activo para {usuario} del libro {codigo}"
                }
            
            if prestamo['renovaciones'] >= 2:
                conn.close()
                return {
                    "exito": False,
                    "mensaje": "Ya se realizaron las 2 renovaciones m�ximas permitidas"
                }
            
            # Actualizar fechas
            fecha_actual = datetime.strptime(prestamo['fecha_devolucion'], "%Y-%m-%d")
            nueva_fecha = fecha_actual + timedelta(weeks=1)
            nueva_fecha_str = nueva_fecha.strftime("%Y-%m-%d")
            nuevas_renovaciones = prestamo['renovaciones'] + 1
            
            cursor.execute(
                "UPDATE prestamos SET fecha_devolucion = ?, renovaciones = ? WHERE codigo = ? AND usuario = ?",
                (nueva_fecha_str, nuevas_renovaciones, codigo, usuario)
            )
            
            # Obtener t�tulo
            cursor.execute("SELECT titulo FROM libros WHERE codigo = ?", (codigo,))
            libro = cursor.fetchone()
            
            conn.commit()
            conn.close()
            
            # Replicar
            self.replicar_operacion({
                "tipo": "renovacion",
                "codigo": codigo,
                "usuario": usuario,
                "nueva_fecha": nueva_fecha_str,
                "renovaciones": nuevas_renovaciones
            })
            
            return {
                "exito": True,
                "mensaje": f"Renovaci�n {nuevas_renovaciones}/2 de '{libro['titulo']}' realizada",
                "nueva_fecha": nueva_fecha_str
            }
        
        except Exception as e:
            conn.rollback()
            conn.close()
            return {
                "exito": False,
                "mensaje": f"Error en renovaci�n: {str(e)}"
            }
    
    def procesar_solicitud(self, solicitud):
        """Procesa solicitudes de Actores/GC"""
        operacion = solicitud.get("operacion")
        
        # Health Check
        if operacion == "health_check":
            return {"status": "ok", "sede": self.sede}
        
        elif operacion == "verificar_disponibilidad":
            return self.verificar_disponibilidad(solicitud["codigo"])
        
        elif operacion == "prestamo":
            return self.realizar_prestamo(solicitud["codigo"], solicitud["usuario"])
        
        elif operacion == "devolucion":
            return self.realizar_devolucion(solicitud["codigo"], solicitud["usuario"])
        
        elif operacion == "renovacion":
            return self.realizar_renovacion(solicitud["codigo"], solicitud["usuario"])
        
        else:
            return {
                "exito": False,
                "mensaje": f"Operaci�n desconocida: {operacion}"
            }
    
    def ejecutar(self):
        """Loop principal del GA"""
        print("=� Gestor de Almacenamiento listo para recibir solicitudes...\n")
        
        while True:
            try:
                # Recibir solicitud
                mensaje = self.socket_rep.recv_string()
                solicitud = json.loads(mensaje)
                
                print(f"=� Solicitud recibida: {solicitud['operacion']}")
                
                # Procesar
                respuesta = self.procesar_solicitud(solicitud)
                
                # Responder
                self.socket_rep.send_string(json.dumps(respuesta))
                
                if respuesta.get("exito", False) or respuesta.get("disponible", False) or respuesta.get("status") == "ok":
                    print(f" {respuesta.get('mensaje', 'OK')}\n")
                else:
                    print(f"L {respuesta.get('mensaje', 'Error')}\n")
                
            except KeyboardInterrupt:
                print("\n=� Deteniendo Gestor de Almacenamiento...")
                break
            except Exception as e:
                print(f"L Error: {e}\n")
                respuesta = {"exito": False, "mensaje": str(e)}
                try:
                    self.socket_rep.send_string(json.dumps(respuesta))
                except:
                    pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python gestor_almacenamiento.py <sede>")
        print("Ejemplo: python gestor_almacenamiento.py 1")
        sys.exit(1)
    
    sede = int(sys.argv[1])
    
    # Configuración por sede
    if sede == 1:
        puerto_rep = "5557"
        replica_ip = "10.43.103.132"  # IP de Comp 2
        replica_port = "5560"  # Puerto PULL en Comp 2 (donde Sede 1 envía)
    else:  # sede == 2
        puerto_rep = "5558"
        replica_ip = "10.43.103.177"  # IP de Comp 1
        replica_port = "5559"  # Puerto PULL en Comp 1 (donde Sede 2 envía)
    
    ga = GestorAlmacenamiento(
        sede=sede,
        puerto_rep=puerto_rep,
        replica_ip=replica_ip,
        replica_port=replica_port
    )
    
    ga.ejecutar()