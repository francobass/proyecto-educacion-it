# proyecto-educacion-it

Repositorio para el proyecto integrador del curso de ingeniería de datos de Educación IT.

## Prerrequisitos

- Git
- Docker
- Python


## Git

Clonar el repositorio:

```bash
git clone https://github.com/francobass/proyecto-educacion-it
```

Comandos habituales:

```bash
git pull
git branch
git checkout -b feature/whale-alert main
git add .
git commit -m "Mensaje del commit"
git checkout main
git push
```

## Entorno virtual

Dentro de la carpeta del proyecto, crear y activar el entorno virtual.

### Windows

Crear el entorno virtual (solo la primera vez):

```bash
py -m venv .venv
```

Activar el entorno virtual:

```bash
.venv\Scripts\activate
```

### Linux

Crear el entorno virtual (solo la primera vez):

```bash
python3 -m venv .venv
```

Activar el entorno virtual:

```bash
source .venv/bin/activate
```

### Instalar paquetes

Ejecutar siempre con el entorno virtual activado (debe verse `(.venv)` a la izquierda de la consola):

```bash
pip install -r requirements.txt
```

## Docker


Levantar servicios:

```bash
docker compose -f minio-docker-compose.yaml up -d
docker compose -f postgres-docker-compose.yaml up -d
docker compose -f airflow-docker-compose.yaml up -d
```

Comandos comunes:
```bash
docker ps
docker compose up -d
docker compose down
docker compose start
docker compose stop
```
## Arquitectura

Mapa de la arquitectura de la plataforma de datos:
https://app.diagrams.net/#G1j_DEIspJAXuBSPLlezS1XO-OoCya_XeN
