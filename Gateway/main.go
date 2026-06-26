package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pbBroker "Broker/proto/Broker"
	pbDatanode "Datanode/proto/Datanode"
	pbGateway "Gateway/proto/Gateway"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	lastInteraction int64
	interactionMu   sync.Mutex
)

func setLastInteraction(val int64) {
	interactionMu.Lock()
	defer interactionMu.Unlock()
	lastInteraction = val
}

func getLastInteraction() int64 {
	interactionMu.Lock()
	defer interactionMu.Unlock()
	return lastInteraction
}

type SessionInfo struct {
	DatanodeAddr string
	ExpiresAt    time.Time
}

type server struct {
	pbGateway.UnimplementedGatewayServiceServer
	brokerClient    pbBroker.BrokerServiceClient
	datanodeClients map[string]pbDatanode.DatanodeServiceClient
	datanodesAddrs  []string

	mu       sync.RWMutex
	sessions map[string]SessionInfo
}

// getRandomDatanodeAddr elige una dirección de Datanode aleatoria
func (s *server) getRandomDatanodeAddr() string {
	if len(s.datanodesAddrs) == 0 {
		return ""
	}
	return s.datanodesAddrs[rand.Intn(len(s.datanodesAddrs))]
}

func (s *server) CrearPedido(ctx context.Context, req *pbGateway.CrearPedidoRequest) (*pbGateway.CrearPedidoResponse, error) {
	setLastInteraction(time.Now().Unix())
	log.Printf("[Gateway] CrearPedido recibido de Cliente: %s, Pedido: %s", req.ClienteId, req.PedidoId)

	targetDatanode := s.getRandomDatanodeAddr()

	// Guardar afinidad de sesión (Read Your Writes)
	s.mu.Lock()
	s.sessions[req.ClienteId] = SessionInfo{
		DatanodeAddr: targetDatanode,
		ExpiresAt:    time.Now().Add(5 * time.Minute), // TTL de 5 minutos
	}
	s.mu.Unlock()

	log.Printf("[Gateway] Afinidad registrada para %s -> %s", req.ClienteId, targetDatanode)

	// Reenviar al Broker
	brokerReq := &pbBroker.EscrituraBrokerRequest{
		PedidoId:       req.PedidoId,
		Estado:         "Recibido", // Estado inicial
		TargetDatanode: targetDatanode,
	}

	resp, err := s.brokerClient.ProcesarEscritura(ctx, brokerReq)
	if err != nil {
		log.Printf("[Gateway] Error al enviar escritura al Broker: %v", err)
		return &pbGateway.CrearPedidoResponse{Exito: false, Mensaje: err.Error()}, nil
	}

	return &pbGateway.CrearPedidoResponse{Exito: resp.Exito, Mensaje: "Pedido creado exitosamente"}, nil
}

func (s *server) ConsultarEstado(ctx context.Context, req *pbGateway.ConsultarEstadoRequest) (*pbGateway.ConsultarEstadoResponse, error) {
	setLastInteraction(time.Now().Unix())
	log.Printf("[Gateway] ConsultarEstado recibido de Cliente: %s, Pedido: %s", req.ClienteId, req.PedidoId)

	s.mu.RLock()
	session, exists := s.sessions[req.ClienteId]
	s.mu.RUnlock()

	if exists && time.Now().Before(session.ExpiresAt) {
		log.Printf("[Gateway] Sesión activa para %s. Redirigiendo lectura a %s", req.ClienteId, session.DatanodeAddr)
		dnClient, ok := s.datanodeClients[session.DatanodeAddr]
		if ok {
			dnReq := &pbDatanode.ObtenerPedidoRequest{PedidoId: req.PedidoId}
			resp, err := dnClient.ObtenerPedido(ctx, dnReq)
			if err != nil {
				log.Printf("[Gateway] Error leyendo de Datanode por afinidad (%v). Haciendo fallback al Broker...", err)
			} else {
				os.MkdirAll("/app/output", 0755)
				f, errLog := os.OpenFile("/app/output/RYW.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if errLog == nil {
					dnName := strings.Split(session.DatanodeAddr, ":")[0]
					if strings.HasPrefix(dnName, "datanode") {
						dnName = "Datanode " + dnName[8:]
					}
					msg := fmt.Sprintf("- %s (%s): Validacion Exitosa en %s (Afinidad de sesion confirmada).\n", req.ClienteId, req.PedidoId, dnName)
					f.WriteString(msg)
					f.Close()
				}

				return &pbGateway.ConsultarEstadoResponse{
					Estado:         resp.Estado,
					RelojVectorial: resp.RelojVectorial,
				}, nil
			}
		}
	}

	log.Printf("[Gateway] Sin sesión activa para %s. Redirigiendo lectura al Broker", req.ClienteId)
	brokerReq := &pbBroker.LecturaBrokerRequest{PedidoId: req.PedidoId}
	resp, err := s.brokerClient.ProcesarLectura(ctx, brokerReq)
	if err != nil {
		log.Printf("[Gateway] Error leyendo del Broker: %v", err)
		return nil, err
	}

	return &pbGateway.ConsultarEstadoResponse{
		Estado:         resp.Estado,
		RelojVectorial: resp.RelojVectorial,
	}, nil
}

func main() {
	// Limpiar log RYW de corridas anteriores para que el reporte no acumule basura
	os.Remove("/app/output/RYW.log")

	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	if port == "" {
		port = "5001"
	}
	log.Printf("%s iniciando en el puerto %s", name, port)

	// Conectar al Broker
	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "broker:5000"
	}
	connBroker, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker %s: %v", brokerAddr, err)
	}
	log.Printf("[Gateway] Conectado al Broker: %s", brokerAddr)
	brokerClient := pbBroker.NewBrokerServiceClient(connBroker)

	// Conectar a los Datanodes
	datanodesEnv := os.Getenv("DATANODES")
	datanodeClients := make(map[string]pbDatanode.DatanodeServiceClient)
	var datanodesAddrs []string

	if datanodesEnv != "" {
		addrs := strings.Split(datanodesEnv, ",")
		for _, addr := range addrs {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("No se pudo conectar a %s: %v", addr, err)
			}
			log.Printf("[Gateway] Conectado al Datanode: %s", addr)
			datanodesAddrs = append(datanodesAddrs, addr)
			datanodeClients[addr] = pbDatanode.NewDatanodeServiceClient(conn)
		}
	}

	setLastInteraction(time.Now().Unix())

	// Hilo para detectar inactividad y apagar el Gateway también
	go func() {
		for {
			time.Sleep(5 * time.Second)
			diff := time.Now().Unix() - getLastInteraction()
			// Si pasan 15 segundos sin actividad (dando tiempo al Broker de cerrar primero), se apaga.
			if diff >= 15 {
				log.Println("[Gateway] 15 segundos de inactividad detectada. Cerrando proceso.")
				os.Exit(0)
			}
		}
	}()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error al abrir el puerto %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	pbGateway.RegisterGatewayServiceServer(grpcServer, &server{
		brokerClient:    brokerClient,
		datanodeClients: datanodeClients,
		datanodesAddrs:  datanodesAddrs,
		sessions:        make(map[string]SessionInfo),
	})

	log.Println("[Gateway] Listo y escuchando mensajes por gRPC.")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Error crítico al servir gRPC: %v", err)
	}
}
