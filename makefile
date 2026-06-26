# Makefile para DiscoPass

build-protoc:
	protoc --go_out=Broker/proto --go-grpc_out=Broker/proto Broker/proto/*.proto
	protoc --go_out=Gateway/proto --go-grpc_out=Gateway/proto Gateway/proto/*.proto
	protoc --go_out=Datanode/proto --go-grpc_out=Datanode/proto Datanode/proto/*.proto

tidy-all:
	cd Broker && go mod tidy
	cd Gateway && go mod tidy
	cd Productor && go mod tidy
	cd Cliente && go mod tidy
	cd Datanode && go mod tidy

# En Docker Compose
docker-up:
	docker compose up --build

docker-down:
	docker compose down

# Máquinas Virtuales (según PDF)
# MV1: Broker Central / Productor de Eventos
docker-VM1:
	docker compose up --build broker productor

# MV2: Gateway de Pedidos / Cliente Hambriento 1 / Datanode 1
docker-VM2:
	docker compose up --build gateway cliente1 datanode1

# MV3: Cliente Hambriento 2 / Datanode 2
docker-VM3:
	docker compose up --build cliente2 datanode2

# MV4: Cliente Hambriento 3 / Datanode 3
docker-VM4:
	docker compose up --build cliente3 datanode3
