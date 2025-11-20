import sqlite3
import sys

def sincronizar_libros(sede):
    """Copia los libros de la BD primaria a la réplica"""
    
    bd_primaria = f"bd_sede{sede}.db"
    bd_replica = f"bd_sede{sede}_replica.db"
    
    print(f"📚 Sincronizando libros de {bd_primaria} a {bd_replica}...")
    
    try:
        # Conectar a BD primaria
        conn_primaria = sqlite3.connect(bd_primaria)
        cursor_primaria = conn_primaria.cursor()
        
        # Conectar a BD réplica
        conn_replica = sqlite3.connect(bd_replica)
        cursor_replica = conn_replica.cursor()
        
        # Obtener todos los libros de la primaria
        cursor_primaria.execute("SELECT * FROM libros")
        libros = cursor_primaria.fetchall()
        
        print(f"📖 Encontrados {len(libros)} libros en BD primaria")
        
        # Limpiar libros existentes en réplica (si hay)
        cursor_replica.execute("DELETE FROM libros")
        
        # Copiar libros a réplica
        cursor_replica.executemany(
            "INSERT INTO libros (codigo, titulo, autor, ejemplares_totales, ejemplares_disponibles) VALUES (?, ?, ?, ?, ?)",
            libros
        )
        
        conn_replica.commit()
        
        # Verificar
        cursor_replica.execute("SELECT COUNT(*) FROM libros")
        count = cursor_replica.fetchone()[0]
        
        print(f"✅ Sincronización completa: {count} libros copiados a la réplica")
        
        conn_primaria.close()
        conn_replica.close()
        
    except Exception as e:
        print(f"❌ Error durante sincronización: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "_main_":
    if len(sys.argv) < 2:
        print("Uso: python sincronizar_replica.py <sede>")
        print("Ejemplo: python sincronizar_replica.py 1")
        sys.exit(1)
    
    sede = int(sys.argv[1])
    sincronizar_libros(sede)
