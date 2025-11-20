# Proyecto Distribuidos

Sistema distribuido implementado en Python, compuesto por varios procesos que interactúan entre sí para manejar solicitudes, almacenamiento, sincronización y monitoreo.

## 📁 Estructura del proyecto

El repositorio contiene varios componentes principales:

- `actor.py`
- `actor_prestamo.py`
- `generador_solicitudes.py`
- `gestor_almacenamiento.py`
- `gestor_carga.py`
- `monitor_ga.py`
- `monitor_gc.py`
- `proceso_solicitante.py`
- `receptor_replica.py`
- `sincronizar_replica.py`
- Archivos de prueba: `solicitudes1.txt`, `test.txt`

Cada archivo corresponde a un proceso dentro del sistema distribuido.

---

## ✅ Requisitos

- Python **3.x**
- libreria **pyzmq**



---

## 🚀 Cómo ejecutarlo

### 1. Ejecutar sede 1

#### Ejecutar el gestor de almacenamiento
```bash
python3 gestor_almacenamiento.py 1
```
#### Ejecutar el gestor de carga
```bash
python3 gestor_carga.py 1
```
#### Ejecutar actor de devolucion
```bash
python3 actor.py devolucion tcp://(ip_Sede_1) (puertoEntrada) (puertoSalida)
```
#### Ejecutar actor de renovacion
```bash
python3 actor.py renovacion tcp://(ip_Sede_1) (puertoEntrada) (puertoSalida)
```
#### Ejecutar actor de prestamo
```bash
python3 actor_prestamo.py tcp://(ip_Sede_1) (puertoEntrada) (puertoSalida)
```
#### Ejecutar Sincronizar Replica
```bash
python3 
```

### 2. Ejecutar sede 2

#### Ejecutar el gestor de almacenamiento
```bash
python3 gestor_almacenamiento.py 2
```
#### Ejecutar el gestor de carga
```bash
python3 gestor_carga.py 2
```
#### Ejecutar actor de devolucion
```bash
python3 actor.py devolucion tcp://(ip_Sede_2) (puertoEntrada) (puertoSalida)
```
#### Ejecutar actor de renovacion
```bash
python3 actor.py renovacion tcp://(ip_Sede_2) (puertoEntrada) (puertoSalida)
```
#### Ejecutar actor de prestamo
```bash
python3 actor_prestamo.py tcp://(ip_Sede_2) (puertoEntrada) (puertoSalida)
```
#### Ejecutar Sincronizar Replica
```bash
python3 
```

### 3. Ejecutar Monitores

#### Ejecutar Monitor gestor almacenamiento y Base de datos
```bash
python3 monitor_ga.py (numero de sede)
```
#### Ejecutar Monitor gestor de carga
```bash
python3 monitor_gc.py (numero de sede)
```


