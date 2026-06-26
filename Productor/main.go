package main

import (
	"context"
	"encoding/csv"
	"log"
	"math/rand"
	"os"
	"time"

	pbBroker "Broker/proto/Broker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	name := os.Getenv("NAME")
	if name == "" {
		name = "Productor"
	}
	log.Printf("%s iniciando...", name)

	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "broker:5000"
	}

	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker: %v", err)
	}
	defer conn.Close()

	client := pbBroker.NewBrokerServiceClient(conn)

	file, err := os.Open("input/pedidos.csv")
	if err != nil {
		log.Fatalf("No se pudo abrir el archivo CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Error leyendo CSV: %v", err)
	}

	relojes := make(map[string][]int32) // pedido_id -> reloj vectorial

	// Ignorar encabezado
	if len(records) > 0 {
		records = records[1:]
	}

	log.Println("[Productor] Iniciando emisión de eventos...")

	for _, row := range records {
		if len(row) < 4 {
			continue
		}
		pedidoID := row[0]
		estado := row[3]

		reloj, exists := relojes[pedidoID]
		if !exists {
			reloj = []int32{0, 0, 0}
		}
		
		// El productor incrementa la posición 0 simulando ser el origen de los cambios
		reloj[0]++
		relojes[pedidoID] = reloj

		req := &pbBroker.ActualizacionProductorRequest{
			PedidoId:       pedidoID,
			Estado:         estado,
			RelojVectorial: reloj,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.ActualizarPedido(ctx, req)
		if err != nil {
			log.Printf("[Productor] Error al enviar evento de %s: %v", pedidoID, err)
		} else {
			log.Printf("[Productor] Evento enviado: Pedido %s -> %s", pedidoID, estado)
		}
		cancel()

		// Simular llegada realista (1 a 3 segundos)
		time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)
	}

	log.Println("[Productor] Todos los eventos han sido emitidos. Terminando proceso.")
	os.Exit(0)
}
