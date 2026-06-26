# INF343-Laboratorio-3
Repositorio conteniendo el desarrollo del laboratorio 3 del ramo de sistemas distribuidos.

### Instalación
Cada nodo es contenido en su propio Container Docker. Una ves con Docker Engine + Compose preparado, se disponen los siguientes comandos en una terminal alocado en la raiz del proyecto:

**Protocol Buffer**
No debe ser necesario compilar los protoc, dado que vienen compilado por defecto, en caso de necesitarlo se dispone de la siguiente linea:

- `make build-protoc`

#### VM1
Maquina VM-25, IP:10.35.168.35

Contiene el servicio de Broker + Productor de Eventos. Para ejecutar use:
- `make docker-VM1`

#### VM2
Maquina VM-26, IP:10.35.168.36

Contiene el servicio de Gateway + Cliente 1 + Datanode 1. Para ejecutar use:
- `make docker-VM2`


#### VM3
Maquina VM-27, IP:10.35.168.37

Contiene el servicio de Cliente 2 + Datanode 2. Para ejecutar use:
- `make docker-VM3`


#### VM4
Maquina VM-28, IP:10.35.168.38

Contiene el servicio de Cliente 3 + Datanode 3. Para ejecutar use:
- `make docker-VM4`
