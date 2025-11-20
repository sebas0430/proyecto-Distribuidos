import random

TIPOS = ["prestamo", "devolucion", "renovacion"]
USUARIOS = [f"user{str(i)}" for i in range(1, 50)]  # Usuarios del 1 al 49
LIBROS = [f"ISBN{str(i).zfill(4)}" for i in range(1, 1001)]  # Libros del 0001 al 1000

def generar_solicitudes(n_lineas=20, archivo_salida="solicitudes1.txt"):
    solicitudes = []

    for _ in range(n_lineas):
        tipo = random.choice(TIPOS)
        usuario = random.choice(USUARIOS)
        libro = random.choice(LIBROS)
        solicitudes.append(f"{tipo},{usuario},{libro}")

    # Guardar en archivo
    with open(archivo_salida, "w", encoding="utf-8") as f:
        for linea in solicitudes:
            f.write(linea + "\n")

    print(f" Archivo generado: {archivo_salida} con {n_lineas} líneas")

if __name__ == "__main__":
    generar_solicitudes()