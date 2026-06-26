package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "Datanode/proto/Datanode"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedDatanodeServiceServer
	storage *MemoryStorage
	idNodo  string // "Datanode1", "Datanode2" o "Datanode3"
	index   int    // Índice asignado: 0, 1 o 2 (para el slice de tamaño 3)
}

type RelojVectorial []int32

type Pedido struct {
	ID             string
	Estado         string
	RelojVectorial RelojVectorial
}

type MemoryStorage struct {
	mu      sync.RWMutex
	pedidos map[string]Pedido
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		pedidos: make(map[string]Pedido),
	}
}

// Lógica principal y consistencia eventual
func (ms *MemoryStorage) GuardarOActualizarPedido(id string, nuevoEstado string, relojEntrante RelojVectorial) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	pedidoExistente, existe := ms.pedidos[id]

	if !existe {
		ms.pedidos[id] = Pedido{
			ID:             id,
			Estado:         nuevoEstado,
			RelojVectorial: ClonarVector(relojEntrante),
		}
		log.Printf("[STORAGE] Nuevo pedido %s guardado en estado: %s", id, nuevoEstado)
		return
	}

	relacionCausal := predecesorVector(pedidoExistente.RelojVectorial, relojEntrante)

	switch relacionCausal {
	case 1:
		ms.pedidos[id] = Pedido{
			ID:             id,
			Estado:         nuevoEstado,
			RelojVectorial: ClonarVector(relojEntrante),
		}
		log.Printf("[ESCRITURA] Pedido %s actualizado causalmente a: %s", id, nuevoEstado)

	case 0:
		mixVector(pedidoExistente.RelojVectorial, relojEntrante)
		ms.pedidos[id] = pedidoExistente
		log.Printf("[ESCRITURA] Pedido %s: Se ignoró estado antiguo (%s). Reloj actualizado.", id, nuevoEstado)

	case -1:
		prioridadLocal := obtenerPrioridadEstado(pedidoExistente.Estado)
		prioridadEntrante := obtenerPrioridadEstado(nuevoEstado)

		log.Printf("[CONFLICTO] Concurrencia detectada en %s. Local: %s vs Entrante: %s", id, pedidoExistente.Estado, nuevoEstado)

		mixVector(pedidoExistente.RelojVectorial, relojEntrante)

		if prioridadEntrante > prioridadLocal {
			pedidoExistente.Estado = nuevoEstado
			log.Printf("[RESOLUCIÓN] Ganó el estado entrante: %s", nuevoEstado)
		} else {
			log.Printf("[RESOLUCIÓN] Se mantuvo el estado local: %s", pedidoExistente.Estado)
		}

		ms.pedidos[id] = pedidoExistente
	}
}

func mixVector(reloj1 RelojVectorial, reloj2 RelojVectorial) {
	for i := 0; i < len(reloj1); i++ {
		if reloj2[i] > reloj1[i] {
			reloj1[i] = reloj2[i]
		}
	}
}

func predecesorVector(reloj1 RelojVectorial, reloj2 RelojVectorial) int32 {
	a := false // Indica si el local es mayor en algún índice
	b := false // Indica si el entrante es mayor en algún índice

	for i := 0; i < len(reloj1); i++ {
		if reloj1[i] > reloj2[i] {
			a = true
		} else if reloj2[i] > reloj1[i] {
			b = true
		}
	}

	if a == false && b == true {
		return 1 // El dato entrante es estrictamente más nuevo
	} else if (a == true && b == false) || (a == false && b == false) {
		return 0 // El dato entrante es antiguo o duplicado
	}
	return -1 // Conflicto de escritura (Concurrentes)
}

func ClonarVector(reloj RelojVectorial) RelojVectorial {
	copia := make(RelojVectorial, 3) // Tamaño estricto N=3 para los 3 Datanodes
	copy(copia, reloj)
	return copia
}

func obtenerPrioridadEstado(estado string) int32 {
	switch estado {
	case "Cancelado":
		return 5
	case "Entregado":
		return 4
	case "En Camino":
		return 3
	case "Preparando":
		return 2
	case "Recibido":
		return 1
	default:
		return 0
	}
}

func NewDatanodeServer(id string, idx int) *server {
	return &server{
		storage: NewMemoryStorage(),
		idNodo:  id,
		index:   idx,
	}
}

func (s *server) EscribirPedido(ctx context.Context, req *pb.EscribirPedidoRequest) (*pb.EscribirPedidoResponse, error) {
	log.Printf("[%s] gRPC EscribirPedido recibido. Pedido: %s, Estado: %s", s.idNodo, req.PedidoId, req.Estado)
	relojEntrante := RelojVectorial(req.RelojVectorial)

	// Como este es un evento directo (escritura nueva) y no un gossip, el nodo avanza su reloj lógico
	if len(relojEntrante) > s.index {
		relojEntrante[s.index]++
	}

	s.storage.GuardarOActualizarPedido(req.PedidoId, req.Estado, relojEntrante)
	return &pb.EscribirPedidoResponse{Exito: true}, nil
}

func (s *server) ObtenerPedido(ctx context.Context, req *pb.ObtenerPedidoRequest) (*pb.ObtenerPedidoResponse, error) {
	log.Printf("[%s] gRPC ObtenerPedido solicitado para ID: %s", s.idNodo, req.PedidoId)
	s.storage.mu.RLock()
	pedido, existe := s.storage.pedidos[req.PedidoId]
	s.storage.mu.RUnlock()

	if !existe {
		return &pb.ObtenerPedidoResponse{
			PedidoId:       req.PedidoId,
			Estado:         "No Encontrado",
			RelojVectorial: []int32{0, 0, 0},
		}, nil
	}

	return &pb.ObtenerPedidoResponse{
		PedidoId:       pedido.ID,
		Estado:         pedido.Estado,
		RelojVectorial: []int32(pedido.RelojVectorial),
	}, nil
}

func (s *server) IntercambioGossip(ctx context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	log.Printf("[%s] Sincronización Gossip entrante recibida de un par.", s.idNodo)
	for pedidoID, datosPedido := range req.CatalogoPedidos {
		relojEntrante := RelojVectorial(datosPedido.RelojVectorial)
		s.storage.GuardarOActualizarPedido(pedidoID, datosPedido.Estado, relojEntrante)
	}

	s.storage.mu.RLock()
	catalogoToSend := make(map[string]*pb.EscribirPedidoRequest)
	for id, ped := range s.storage.pedidos {
		catalogoToSend[id] = &pb.EscribirPedidoRequest{
			PedidoId:       ped.ID,
			Estado:         ped.Estado,
			RelojVectorial: []int32(ped.RelojVectorial),
		}
	}
	s.storage.mu.RUnlock()

	return &pb.GossipResponse{Ack: true, CatalogoPedidos: catalogoToSend}, nil
}

func (s *server) MandarAuditoria(ctx context.Context, req *pb.AuditoriaRequest) (*pb.AuditoriaResponse, error) {
	log.Printf("[%s] Fase 5: Ejecutando volcado de Auditoría Final...", s.idNodo)

	s.storage.mu.RLock()
	defer s.storage.mu.RUnlock()

	err := os.MkdirAll("/app/output", 0755)
	if err != nil {
		log.Printf("Error creando directorio /app/output: %v", err)
	}

	nombreArchivo := fmt.Sprintf("/app/output/%s_output.txt", s.idNodo)
	file, err := os.Create(nombreArchivo)
	if err == nil {
		var llavesOrdenadas []string
		for k := range s.storage.pedidos {
			llavesOrdenadas = append(llavesOrdenadas, k)
		}
		sort.Strings(llavesOrdenadas)

		for _, id := range llavesOrdenadas {
			ped := s.storage.pedidos[id]
			linea := fmt.Sprintf("Pedido ID: %s | Estado Final: %s | Reloj Vectorial: %v\n",
				ped.ID, ped.Estado, []int32(ped.RelojVectorial))
			file.WriteString(linea)
		}
		file.Close()
		log.Printf("[%s] Estado local guardado en %s.", s.idNodo, nombreArchivo)
	}

	// Preparar el catálogo para enviarlo de vuelta al Broker
	catalogoToSend := make(map[string]*pb.EscribirPedidoRequest)
	for id, ped := range s.storage.pedidos {
		catalogoToSend[id] = &pb.EscribirPedidoRequest{
			PedidoId:       ped.ID,
			Estado:         ped.Estado,
			RelojVectorial: []int32(ped.RelojVectorial),
		}
	}

	// Apagar el datanode dando un pequeño margen para responder el gRPC
	go func() {
		time.Sleep(2 * time.Second)
		log.Printf("[%s] Apagando el Datanode tras enviar catálogo al Broker.", s.idNodo)
		os.Exit(0)
	}()

	return &pb.AuditoriaResponse{
		ArchivoCreado: true,
		Catalogo:      catalogoToSend,
	}, nil
}

func (s *server) IniciarRutinaGossip() {
	peersEnv := os.Getenv("PEERS")
	if peersEnv == "" {
		log.Printf("[%s] Advertencia: No se detectaron PEERS en el entorno.", s.idNodo)
		return
	}
	direccionesPares := strings.Split(peersEnv, ",")
	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {
		s.storage.mu.RLock()
		if len(s.storage.pedidos) == 0 {
			s.storage.mu.RUnlock()
			continue
		}

		catalogoToSend := make(map[string]*pb.EscribirPedidoRequest)
		for id, ped := range s.storage.pedidos {
			catalogoToSend[id] = &pb.EscribirPedidoRequest{
				PedidoId:       ped.ID,
				Estado:         ped.Estado,
				RelojVectorial: []int32(ped.RelojVectorial),
			}
		}
		s.storage.mu.RUnlock()

		targetPeer := direccionesPares[rand.Intn(len(direccionesPares))]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, targetPeer, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			cancel()
			continue
		}

		client := pb.NewDatanodeServiceClient(conn)
		resp, errGossip := client.IntercambioGossip(ctx, &pb.GossipRequest{CatalogoPedidos: catalogoToSend})

		if errGossip == nil && resp.CatalogoPedidos != nil {
			for pedidoID, datosPedido := range resp.CatalogoPedidos {
				relojEntrante := RelojVectorial(datosPedido.RelojVectorial)
				s.storage.GuardarOActualizarPedido(pedidoID, datosPedido.Estado, relojEntrante)
			}
		}

		conn.Close()
		cancel()
	}
}

func main() {
	name := os.Getenv("NAME")
	port := ":" + os.Getenv("PORT")
	indexEnv := os.Getenv("INDEX")

	idx, _ := strconv.Atoi(indexEnv)
	log.Printf("%s inicializado en el puerto %s", name, port)

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Fallo al abrir puerto: %v", err)
	}

	grpcServer := grpc.NewServer()
	datanodeServer := NewDatanodeServer(name, idx)
	pb.RegisterDatanodeServiceServer(grpcServer, datanodeServer)

	go datanodeServer.IniciarRutinaGossip() // Arranca el bucle de sincronización eventual asíncrono

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Error crítico gRPC: %v", err)
	}
}
