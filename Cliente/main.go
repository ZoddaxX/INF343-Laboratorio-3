package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	pbGateway "Gateway/proto/Gateway"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	name := os.Getenv("NAME")
	if name == "" {
		name = "Cliente"
	}
	log.Printf("%s iniciando...", name)

	gatewayAddr := os.Getenv("GATEWAY_ADDR")
	if gatewayAddr == "" {
		gatewayAddr = "gateway:5001"
	}

	// Esperar unos segundos para que Gateway y Datanodes arranquen
	time.Sleep(5 * time.Second)

	conn, err := grpc.Dial(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Gateway: %v", err)
	}
	defer conn.Close()

	client := pbGateway.NewGatewayServiceClient(conn)

	// Simular comportamiento durante un periodo de tiempo
	for i := 1; i <= 3; i++ {
		pedidoID := fmt.Sprintf("Req-%s-%d", name, i)

		log.Printf("\n[%s] --- Iniciando Transacción para %s ---", name, pedidoID)

		ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.ConsultarEstado(ctx1, &pbGateway.ConsultarEstadoRequest{
			ClienteId: name,
			PedidoId:  "Menu-Inicial",
		})
		cancel1()
		if err != nil {
			log.Printf("[%s] Solicitud inicial falló: %v", name, err)
		} else {
			log.Printf("[%s] Solicitud inicial (Menú) procesada exitosamente.", name)
		}

		log.Printf("[%s] Enviando solicitud CrearPedido para %s...", name, pedidoID)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		respWrite, err := client.CrearPedido(ctx2, &pbGateway.CrearPedidoRequest{
			ClienteId: name,
			PedidoId:  pedidoID,
		})
		cancel2()
		if err != nil {
			log.Printf("[%s] Error al crear pedido: %v", name, err)
			continue
		}
		if !respWrite.Exito {
			log.Printf("[%s] Gateway rechazó la creación del pedido.", name)
			continue
		}
		log.Printf("[%s] Pedido %s creado exitosamente. Ejecutando lectura de confirmación (RYW)...", name, pedidoID)

		ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
		respRead, err := client.ConsultarEstado(ctx3, &pbGateway.ConsultarEstadoRequest{
			ClienteId: name,
			PedidoId:  pedidoID,
		})
		cancel3()
		if err != nil {
			log.Printf("[%s] Error al consultar estado de confirmación: %v", name, err)
			continue
		}

		if respRead.Estado == "Recibido" {
			log.Printf("[%s] Validacion Exitosa (RYW cumplido). Estado actual: %s", name, respRead.Estado)
		} else {
			log.Printf("[%s] ADVERTENCIA: Validacion Fallida (RYW roto). Estado devuelto: %s", name, respRead.Estado)
		}

		log.Printf("[%s] --- Transacción %s Finalizada ---\n", name, pedidoID)

		// Esperar antes del siguiente pedido
		time.Sleep(time.Duration(rand.Intn(5)+5) * time.Second)
	}

	log.Printf("[%s] Todas las transacciones terminadas. Terminando proceso.", name)
	os.Exit(0)
}
