import zmq
import sqlite3
import time
import os
import threading
from datetime import datetime, timedelta
from contextlib import contextmanager

class GestorAlmacenamiento:
    def __init__(self, sede, puerto_req="5557", db_principal="bd_sede1.db", db_replica="bd_sede1_replica.db"):
        """
        Gestor de Almacenamiento y Persistencia con SQLite
        
        Args:
            sede: número de sede (1 o 2)
            puerto_req: puerto para recibir solicitudes de Actores
            db_principal: archivo SQLite de BD principal
            db_replica: archivo SQLite de BD réplica
        """
        self.sede = sede
        self.db_principal = db_principal
        self.db_replica = db_replica
        self.context = zmq.Context()
        self.lock = threading.Lock()  # Para operaciones thread-safe
        
        # Socket REP para recibir solicitudes de Actores
        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.bind(f"tcp://*:{puerto_req}")
        
        # Inicializar BD si no existe
        self.inicializar_bd()
        
        print(f"💾 Gestor de Almacenamiento Sede {sede} iniciado")
        print(f"📡 Escuchando en puerto {puerto_req}")
        print(f"📚 BD Principal: {db_principal}")
        print(f"📚 BD Réplica: {db_replica}\n")
    
    @contextmanager
    def get_db_connection(self, db_file):
        """Context manager para conexiones de BD"""
        conn = sqlite3.connect(db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row  # Para acceder a columnas por nombre
        try:
            yield conn
        finally:
            conn.close()
    
    def inicializar_bd(self):
        """Crea las tablas y datos iniciales si no existen"""
        # Inicializar BD principal
        if not os.path.exists(self.db_principal):
            print("🔨 Generando Base de Datos SQLite inicial...")
            self._crear_esquema(self.db_principal)
            self._poblar_datos_iniciales(self.db_principal)
            print(f"✅ BD principal generada\n")
        
        # Inicializar réplica (copia de la principal)
        if not os.path.exists(self.db_replica):
            print("🔨 Creando réplica inicial...")
            self._crear_esquema(self.db_replica)
            self._copiar_datos_a_replica()
            print(f"✅ BD réplica generada\n")
    
    def _crear_esquema(self, db_file):
        """Crea las tablas en la BD"""
        with self.get_db_connection(db_file) as conn:
            cursor = conn.cursor()
            
            # Tabla de libros
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS libros (
                    codigo TEXT PRIMARY KEY,
                    titulo TEXT NOT NULL,
                    autor TEXT NOT NULL,
                    ejemplares_totales INTEGER NOT NULL DEFAULT 1,
                    ejemplares_disponibles INTEGER NOT NULL DEFAULT 1,
                    CHECK (ejemplares_disponibles >= 0),
                    CHECK (ejemplares_disponibles <= ejemplares_totales)
                )
            ''')
            
            # Tabla de préstamos activos
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prestamos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    codigo_libro TEXT NOT NULL,
                    usuario TEXT NOT NULL,
                    fecha_prestamo TEXT NOT NULL,
                    fecha_devolucion TEXT NOT NULL,
                    renovaciones INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (codigo_libro) REFERENCES libros(codigo),
                    CHECK (renovaciones >= 0 AND renovaciones <= 2)
                )
            ''')
            
            # Índices para mejorar performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_prestamos_libro 
                ON prestamos(codigo_libro)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_prestamos_usuario 
                ON prestamos(usuario)
            ''')
            
            conn.commit()
    
    def _poblar_datos_iniciales(self, db_file):
        """Llena la BD con 1000 libros iniciales"""
        with self.get_db_connection(db_file) as conn:
            cursor = conn.cursor()
            
            # Generar 1000 libros
            libros = []
            for i in range(1, 1001):
                codigo = f"ISBN{i:04d}"
                titulo = f"Libro {i}"
                autor = f"Autor {(i % 100) + 1}"
                # Algunos libros tienen 1 ejemplar, otros varios
                ejemplares = 1 if i % 10 == 0 else (i % 5) + 1
                libros.append((codigo, titulo, autor, ejemplares, ejemplares))
            
            cursor.executemany('''
                INSERT INTO libros (codigo, titulo, autor, ejemplares_totales, ejemplares_disponibles)
                VALUES (?, ?, ?, ?, ?)
            ''', libros)
            
            # Crear préstamos iniciales (50 sede 1, 150 sede 2)
            num_prestados = 50 if self.sede == 1 else 150
            prestamos = []
            
            for i in range(1, num_prestados + 1):
                codigo = f"ISBN{i:04d}"
                usuario = f"user{i}"
                fecha_prestamo = datetime.now()
                fecha_devolucion = fecha_prestamo + timedelta(weeks=2)
                
                prestamos.append((
                    codigo,
                    usuario,
                    fecha_prestamo.isoformat(),
                    fecha_devolucion.isoformat(),
                    0  # renovaciones
                ))
                
                # Actualizar ejemplares disponibles
                cursor.execute('''
                    UPDATE libros 
                    SET ejemplares_disponibles = ejemplares_disponibles - 1
                    WHERE codigo = ? AND ejemplares_disponibles > 0
                ''', (codigo,))
            
            cursor.executemany('''
                INSERT INTO prestamos (codigo_libro, usuario, fecha_prestamo, fecha_devolucion, renovaciones)
                VALUES (?, ?, ?, ?, ?)
            ''', prestamos)
            
            conn.commit()
            
            print(f"   📚 1000 libros generados")
            print(f"   📖 {num_prestados} libros prestados inicialmente")
    
    def _copiar_datos_a_replica(self):
        """Copia todos los datos de la BD principal a la réplica"""
        with self.get_db_connection(self.db_principal) as conn_principal:
            with self.get_db_connection(self.db_replica) as conn_replica:
                # Copiar libros
                libros = conn_principal.execute('SELECT * FROM libros').fetchall()
                conn_replica.executemany('''
                    INSERT OR REPLACE INTO libros 
                    (codigo, titulo, autor, ejemplares_totales, ejemplares_disponibles)
                    VALUES (?, ?, ?, ?, ?)
                ''', [(l['codigo'], l['titulo'], l['autor'], l['ejemplares_totales'], l['ejemplares_disponibles']) 
                      for l in libros])
                
                # Copiar préstamos
                prestamos = conn_principal.execute('SELECT * FROM prestamos').fetchall()
                conn_replica.executemany('''
                    INSERT OR REPLACE INTO prestamos 
                    (id, codigo_libro, usuario, fecha_prestamo, fecha_devolucion, renovaciones)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', [(p['id'], p['codigo_libro'], p['usuario'], p['fecha_prestamo'], 
                       p['fecha_devolucion'], p['renovaciones']) for p in prestamos])
                
                conn_replica.commit()
    
    def actualizar_replica_async(self):
        """Actualiza la réplica de forma asíncrona en un thread separado"""
        def actualizar():
            time.sleep(0.1)  # Simular delay asíncrono
            with self.lock:
                try:
                    # Limpiar réplica
                    with self.get_db_connection(self.db_replica) as conn:
                        conn.execute('DELETE FROM prestamos')
                        conn.execute('DELETE FROM libros')
                        conn.commit()
                    
                    # Copiar datos actualizados
                    self._copiar_datos_a_replica()
                    print("🔄 Réplica actualizada")
                except Exception as e:
                    print(f"❌ Error actualizando réplica: {e}")
        
        # Ejecutar en thread separado para no bloquear
        thread = threading.Thread(target=actualizar, daemon=True)
        thread.start()
    
    def verificar_disponibilidad(self, codigo_libro):
        """Verifica si hay ejemplares disponibles de un libro"""
        with self.get_db_connection(self.db_principal) as conn:
            cursor = conn.cursor()
            
            libro = cursor.execute('''
                SELECT * FROM libros WHERE codigo = ?
            ''', (codigo_libro,)).fetchone()
            
            if not libro:
                return {"disponible": False, "mensaje": "Libro no existe en biblioteca"}
            
            if libro['ejemplares_disponibles'] > 0:
                return {
                    "disponible": True, 
                    "libro": {
                        "codigo": libro['codigo'],
                        "titulo": libro['titulo'],
                        "ejemplares_disponibles": libro['ejemplares_disponibles']
                    }
                }
            else:
                return {"disponible": False, "mensaje": "No hay ejemplares disponibles"}
    
    def procesar_prestamo(self, codigo_libro, usuario):
        """Registra un préstamo en la BD usando transacción"""
        with self.lock:
            with self.get_db_connection(self.db_principal) as conn:
                cursor = conn.cursor()
                
                try:
                    # Iniciar transacción
                    conn.execute('BEGIN IMMEDIATE')
                    
                    # Verificar existencia y disponibilidad
                    libro = cursor.execute('''
                        SELECT * FROM libros WHERE codigo = ?
                    ''', (codigo_libro,)).fetchone()
                    
                    if not libro:
                        conn.rollback()
                        return {"exito": False, "mensaje": "Libro no encontrado"}
                    
                    if libro['ejemplares_disponibles'] <= 0:
                        conn.rollback()
                        return {"exito": False, "mensaje": "Sin ejemplares disponibles"}
                    
                    # Actualizar ejemplares disponibles
                    cursor.execute('''
                        UPDATE libros 
                        SET ejemplares_disponibles = ejemplares_disponibles - 1
                        WHERE codigo = ?
                    ''', (codigo_libro,))
                    
                    # Crear préstamo
                    fecha_prestamo = datetime.now()
                    fecha_devolucion = fecha_prestamo + timedelta(weeks=2)
                    
                    cursor.execute('''
                        INSERT INTO prestamos (codigo_libro, usuario, fecha_prestamo, fecha_devolucion, renovaciones)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (codigo_libro, usuario, fecha_prestamo.isoformat(), 
                          fecha_devolucion.isoformat(), 0))
                    
                    # Confirmar transacción
                    conn.commit()
                    
                    # Actualizar réplica
                    self.actualizar_replica_async()
                    
                    return {
                        "exito": True, 
                        "mensaje": "Préstamo otorgado",
                        "fecha_devolucion": fecha_devolucion.strftime("%Y-%m-%d")
                    }
                    
                except sqlite3.Error as e:
                    conn.rollback()
                    print(f"❌ Error en transacción de préstamo: {e}")
                    return {"exito": False, "mensaje": f"Error en BD: {str(e)}"}
    
    def procesar_devolucion(self, codigo_libro, usuario):
        """Registra una devolución en la BD"""
        with self.lock:
            with self.get_db_connection(self.db_principal) as conn:
                cursor = conn.cursor()
                
                try:
                    conn.execute('BEGIN IMMEDIATE')
                    
                    # Buscar préstamo activo
                    prestamo = cursor.execute('''
                        SELECT * FROM prestamos 
                        WHERE codigo_libro = ? AND usuario = ?
                        LIMIT 1
                    ''', (codigo_libro, usuario)).fetchone()
                    
                    if not prestamo:
                        conn.rollback()
                        return {"exito": False, "mensaje": "No se encontró préstamo activo"}
                    
                    # Eliminar préstamo
                    cursor.execute('''
                        DELETE FROM prestamos WHERE id = ?
                    ''', (prestamo['id'],))
                    
                    # Incrementar ejemplares disponibles
                    cursor.execute('''
                        UPDATE libros 
                        SET ejemplares_disponibles = ejemplares_disponibles + 1
                        WHERE codigo = ?
                    ''', (codigo_libro,))
                    
                    conn.commit()
                    
                    # Actualizar réplica
                    self.actualizar_replica_async()
                    
                    return {"exito": True, "mensaje": "Devolución registrada"}
                    
                except sqlite3.Error as e:
                    conn.rollback()
                    print(f"❌ Error en devolución: {e}")
                    return {"exito": False, "mensaje": f"Error en BD: {str(e)}"}
    
    def procesar_renovacion(self, codigo_libro, usuario):
        """Renueva un préstamo por una semana adicional"""
        with self.lock:
            with self.get_db_connection(self.db_principal) as conn:
                cursor = conn.cursor()
                
                try:
                    conn.execute('BEGIN IMMEDIATE')
                    
                    # Buscar préstamo activo
                    prestamo = cursor.execute('''
                        SELECT * FROM prestamos 
                        WHERE codigo_libro = ? AND usuario = ?
                        LIMIT 1
                    ''', (codigo_libro, usuario)).fetchone()
                    
                    if not prestamo:
                        conn.rollback()
                        return {"exito": False, "mensaje": "No se encontró préstamo activo"}
                    
                    if prestamo['renovaciones'] >= 2:
                        conn.rollback()
                        return {"exito": False, "mensaje": "Máximo de renovaciones alcanzado (2)"}
                    
                    # Calcular nueva fecha
                    fecha_actual = datetime.fromisoformat(prestamo['fecha_devolucion'])
                    nueva_fecha = fecha_actual + timedelta(weeks=1)
                    nuevas_renovaciones = prestamo['renovaciones'] + 1
                    
                    # Actualizar préstamo
                    cursor.execute('''
                        UPDATE prestamos 
                        SET fecha_devolucion = ?, renovaciones = ?
                        WHERE id = ?
                    ''', (nueva_fecha.isoformat(), nuevas_renovaciones, prestamo['id']))
                    
                    conn.commit()
                    
                    # Actualizar réplica
                    self.actualizar_replica_async()
                    
                    return {
                        "exito": True, 
                        "mensaje": "Renovación exitosa",
                        "nueva_fecha": nueva_fecha.strftime("%Y-%m-%d"),
                        "renovaciones": nuevas_renovaciones
                    }
                    
                except sqlite3.Error as e:
                    conn.rollback()
                    print(f"❌ Error en renovación: {e}")
                    return {"exito": False, "mensaje": f"Error en BD: {str(e)}"}
    
    def obtener_estadisticas(self):
        """Obtiene estadísticas de la BD (útil para debugging)"""
        with self.get_db_connection(self.db_principal) as conn:
            cursor = conn.cursor()
            
            total_libros = cursor.execute('SELECT COUNT(*) FROM libros').fetchone()[0]
            total_ejemplares = cursor.execute('SELECT SUM(ejemplares_totales) FROM libros').fetchone()[0]
            total_disponibles = cursor.execute('SELECT SUM(ejemplares_disponibles) FROM libros').fetchone()[0]
            total_prestamos = cursor.execute('SELECT COUNT(*) FROM prestamos').fetchone()[0]
            
            return {
                "total_libros": total_libros,
                "total_ejemplares": total_ejemplares,
                "ejemplares_disponibles": total_disponibles,
                "ejemplares_prestados": total_ejemplares - total_disponibles,
                "prestamos_activos": total_prestamos
            }
    
    def ejecutar(self):
        """Loop principal del GA"""
        print("🚀 Gestor de Almacenamiento listo...\n")
        
        # Mostrar estadísticas iniciales
        stats = self.obtener_estadisticas()
        print(f"📊 Estadísticas iniciales:")
        print(f"   📚 Libros: {stats['total_libros']}")
        print(f"   📦 Ejemplares totales: {stats['total_ejemplares']}")
        print(f"   ✅ Disponibles: {stats['ejemplares_disponibles']}")
        print(f"   📖 Prestados: {stats['ejemplares_prestados']}\n")
        
        while True:
            try:
                # Recibir solicitud de Actor o GC
                import json
                mensaje_json = self.socket_rep.recv_string()
                mensaje = json.loads(mensaje_json)
                
                operacion = mensaje["operacion"]
                codigo = mensaje.get("codigo")
                usuario = mensaje.get("usuario")
                
                print(f"📥 Operación: {operacion} | Usuario: {usuario} | Libro: {codigo}")
                
                # Procesar según tipo de operación
                if operacion == "verificar_disponibilidad":
                    respuesta = self.verificar_disponibilidad(codigo)
                elif operacion == "prestamo":
                    respuesta = self.procesar_prestamo(codigo, usuario)
                elif operacion == "devolucion":
                    respuesta = self.procesar_devolucion(codigo, usuario)
                elif operacion == "renovacion":
                    respuesta = self.procesar_renovacion(codigo, usuario)
                elif operacion == "estadisticas":
                    respuesta = self.obtener_estadisticas()
                else:
                    respuesta = {"exito": False, "mensaje": "Operación desconocida"}
                
                # Enviar respuesta
                self.socket_rep.send_string(json.dumps(respuesta))
                print(f"✅ Respuesta: {respuesta.get('mensaje', 'OK')}\n")
                
            except KeyboardInterrupt:
                print("\n🛑 Deteniendo Gestor de Almacenamiento...")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                self.socket_rep.send_string(json.dumps({"exito": False, "mensaje": str(e)}))

if __name__ == "__main__":
    import sys
    
    print("📦 Iniciando Gestor de Almacenamiento...\n")
    # Configurar según sede
    if len(sys.argv) > 1:
        sede = int(sys.argv[1])
    else:
        sede = 1
    
    # Configuración por sede
    configuraciones = {
        1: {
            "puerto": "5557",
            "db_principal": "bd_sede1.db",
            "db_replica": "bd_sede1_replica.db"
        },
        2: {
            "puerto": "5558",
            "db_principal": "bd_sede2.db",
            "db_replica": "bd_sede2_replica.db"
        }
    }
    
    config = configuraciones[sede]
    ga = GestorAlmacenamiento(
        sede=sede,
        puerto_req=config["puerto"],
        db_principal=config["db_principal"],
        db_replica=config["db_replica"]
    )
    
    ga.ejecutar()