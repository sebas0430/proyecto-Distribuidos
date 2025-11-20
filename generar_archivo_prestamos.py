"""
Genera archivos de solicitudes de préstamo para pruebas de rendimiento
"""

def generar_archivo_prestamos(numero_ps, cantidad=100):
    """
    Genera archivo con solicitudes de préstamo
    
    Args:
        numero_ps: número del proceso solicitante (1-10)
        cantidad: cantidad de solicitudes a generar
    """
    nombre_archivo = f"prestamos_ps{numero_ps}.txt"
    
    # Rango de ISBNs para evitar colisiones
    # PS1: 0101-0200, PS2: 0201-0300, etc.
    inicio_isbn = (numero_ps * 100) + 1
    
    with open(nombre_archivo, 'w') as f:
        for i in range(cantidad):
            isbn = inicio_isbn + i
            usuario = 300 + (numero_ps * 100) + i  # user301, user401, etc.
            f.write(f"prestamo,user{usuario},ISBN{isbn:04d}\n")
    
    print(f" Creado: {nombre_archivo} con {cantidad} solicitudes")
    print(f"   Rango ISBN: ISBN{inicio_isbn:04d} - ISBN{(inicio_isbn + cantidad - 1):04d}")
    print(f"   Rango usuarios: user{300 + (numero_ps * 100)} - user{300 + (numero_ps * 100) + cantidad - 1}")

if __name__ == "__main__":
    print("=" * 60)
    print("GENERANDO ARCHIVOS DE SOLICITUDES PARA PRUEBAS")
    print("=" * 60)
    
    # Generar 10 archivos (uno por cada PS)
    for i in range(1, 11):
        generar_archivo_prestamos(i, cantidad=100)
        print()
    
    print("=" * 60)
    print(" Todos los archivos generados")
    print("=" * 60)