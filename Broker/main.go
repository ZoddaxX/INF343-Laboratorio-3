package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	pbBroker "Broker/proto/Broker"
	pbDatanode "Datanode/proto/Datanode"

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

type server struct {
	pbBroker.UnimplementedBrokerServiceServer
	datanodeClients []pbDatanode.DatanodeServiceClient
	datanodeMap     map[string]pbDatanode.DatanodeServiceClient // URL -> Client
	rrCounter       uint64                                      // Para Round Robin
	rrMu            sync.Mutex
}

func (s *server) getClientByAddr(addr string) pbDatanode.DatanodeServiceClient {
	if client, ok := s.datanodeMap[addr]; ok {
		return client
	}
	for k, v := range s.datanodeMap {
		if strings.HasPrefix(k, addr) {
			return v
		}
	}
	return nil
}

func (s *server) getNextDatanode() pbDatanode.DatanodeServiceClient {
	s.rrMu.Lock()
	s.rrCounter++
	idx := s.rrCounter % uint64(len(s.datanodeClients))
	s.rrMu.Unlock()
	return s.datanodeClients[idx]
}

func (s *server) ProcesarEscritura(ctx context.Context, req *pbBroker.EscrituraBrokerRequest) (*pbBroker.EscrituraBrokerResponse, error) {
	setLastInteraction(time.Now().Unix())
	log.Printf("[Broker] ProcesarEscritura para pedido: %s", req.PedidoId)

	var client pbDatanode.DatanodeServiceClient
	if req.TargetDatanode != "" {
		log.Printf("[Broker] Enrutando por afinidad a: %s", req.TargetDatanode)
		client = s.getClientByAddr(req.TargetDatanode)
		if client == nil {
			log.Printf("[Broker] No se encontró cliente para %s, usando Round Robin", req.TargetDatanode)
			client = s.getNextDatanode()
		}
	} else {
		client = s.getNextDatanode()
	}

	dnReq := &pbDatanode.EscribirPedidoRequest{
		PedidoId:       req.PedidoId,
		Estado:         req.Estado,
		RelojVectorial: []int32{0, 0, 0},
	}

	var resp *pbDatanode.EscribirPedidoResponse
	var err error
	for i := 0; i < len(s.datanodeClients); i++ {
		retryCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err = client.EscribirPedido(retryCtx, dnReq)
		cancel()
		if err == nil {
			break
		}
		log.Printf("[Broker] Error conectando al Datanode. Reintentando con otro... Error: %v", err)
		client = s.getNextDatanode()
	}

	if err != nil {
		log.Printf("[Broker] Error fatal: Ningún Datanode disponible.")
		return nil, err
	}

	return &pbBroker.EscrituraBrokerResponse{Exito: resp.Exito}, nil
}

func (s *server) ProcesarLectura(ctx context.Context, req *pbBroker.LecturaBrokerRequest) (*pbBroker.LecturaBrokerResponse, error) {
	setLastInteraction(time.Now().Unix())
	log.Printf("[Broker] ProcesarLectura para pedido: %s", req.PedidoId)

	client := s.getNextDatanode()
	dnReq := &pbDatanode.ObtenerPedidoRequest{
		PedidoId: req.PedidoId,
	}

	var resp *pbDatanode.ObtenerPedidoResponse
	var err error
	for i := 0; i < len(s.datanodeClients); i++ {
		retryCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err = client.ObtenerPedido(retryCtx, dnReq)
		cancel()
		if err == nil {
			break
		}
		log.Printf("[Broker] Error leyendo del Datanode. Reintentando con otro... Error: %v", err)
		client = s.getNextDatanode()
	}

	if err != nil {
		log.Printf("[Broker] Error fatal: Ningún Datanode disponible para leer.")
		return nil, err
	}

	return &pbBroker.LecturaBrokerResponse{
		Estado:         resp.Estado,
		RelojVectorial: resp.RelojVectorial,
	}, nil
}

func (s *server) ActualizarPedido(ctx context.Context, req *pbBroker.ActualizacionProductorRequest) (*pbBroker.ActualizacionProductorResponse, error) {
	setLastInteraction(time.Now().Unix())
	log.Printf("[Broker] Recibió actualización de productor para pedido: %s, estado: %s", req.PedidoId, req.Estado)

	client := s.getNextDatanode()
	dnReq := &pbDatanode.EscribirPedidoRequest{
		PedidoId:       req.PedidoId,
		Estado:         req.Estado,
		RelojVectorial: req.RelojVectorial,
	}

	var resp *pbDatanode.EscribirPedidoResponse
	var err error
	for i := 0; i < len(s.datanodeClients); i++ {
		retryCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err = client.EscribirPedido(retryCtx, dnReq)
		cancel()
		if err == nil {
			break
		}
		log.Printf("[Broker] Error conectando al Datanode. Reintentando con otro... Error: %v", err)
		client = s.getNextDatanode()
	}

	if err != nil {
		log.Printf("[Broker] Error fatal: Ningún Datanode disponible.")
		return nil, err
	}

	return &pbBroker.ActualizacionProductorResponse{Exito: resp.Exito}, nil
}

func main() {
	name := os.Getenv("NAME")
	port := os.Getenv("PORT")
	if port == "" {
		port = "5000"
	}
	log.Printf("%s iniciando en el puerto %s", name, port)

	datanodesEnv := os.Getenv("DATANODES")
	var clients []pbDatanode.DatanodeServiceClient
	clientsMap := make(map[string]pbDatanode.DatanodeServiceClient)
	if datanodesEnv != "" {
		addrs := strings.Split(datanodesEnv, ",")
		for _, addr := range addrs {
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("No se pudo conectar a %s: %v", addr, err)
			}
			log.Printf("[Broker] Conectado al Datanode: %s", addr)
			client := pbDatanode.NewDatanodeServiceClient(conn)
			clients = append(clients, client)
			clientsMap[addr] = client
		}
	} else {
		log.Println("[Broker] Advertencia: No hay Datanodes configurados.")
	}

	setLastInteraction(time.Now().Unix())

	go func() {
		for {
			time.Sleep(5 * time.Second)
			diff := time.Now().Unix() - getLastInteraction()
			// Si pasan 10 segundos sin actividad, se inicia la auditoria.
			if diff >= 10 {
				log.Println("[Broker] 10 segundos de inactividad detectada. Iniciando Auditoría...")

				catalogos := make(map[string]map[string]*pbDatanode.EscribirPedidoRequest)

				for addr, client := range clientsMap {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					resp, err := client.MandarAuditoria(ctx, &pbDatanode.AuditoriaRequest{})
					if err != nil {
						log.Printf("[Broker] Error solicitando auditoría a %s: %v", addr, err)
					} else {
						log.Printf("[Broker] Auditoría completada en %s", addr)
						if resp.Catalogo != nil {
							catalogos[addr] = resp.Catalogo
						}
					}
					cancel()
				}

				convergencia := true
				var refCatalog map[string]*pbDatanode.EscribirPedidoRequest
				for _, cat := range catalogos {
					if refCatalog == nil {
						refCatalog = cat
						continue
					}
					if len(cat) != len(refCatalog) {
						convergencia = false
						break
					}
					for id, ped := range cat {
						refPed, ok := refCatalog[id]
						if !ok || refPed.Estado != ped.Estado {
							convergencia = false
							break
						}
						for i, v := range ped.RelojVectorial {
							if refPed.RelojVectorial[i] != v {
								convergencia = false
								break
							}
						}
						if !convergencia {
							break
						}
					}
				}

				err := os.MkdirAll("/app/output", 0755)
				if err != nil {
					log.Printf("Error creando directorio /app/output: %v", err)
				}
				file, err := os.Create("/app/output/Reporte.txt")
				if err == nil {
					file.WriteString("=== REPORTE FINAL: DISTRIEATS ===\n")
					if convergencia && refCatalog != nil {
						file.WriteString("[ESTADO GLOBAL DE PEDIDOS - Convergencia Alcanzada]\n")
					} else {
						file.WriteString("[ESTADO GLOBAL DE PEDIDOS - Divergencia Detectada]\n")
					}

					if refCatalog != nil {
						var llavesOrdenadas []string
						for k := range refCatalog {
							llavesOrdenadas = append(llavesOrdenadas, k)
						}
						sort.Strings(llavesOrdenadas)

						for _, id := range llavesOrdenadas {
							ped := refCatalog[id]
							linea := fmt.Sprintf("Pedido ID: %s | Estado Final: %s | Reloj Vectorial: [DN1:%d, DN2:%d, DN3:%d]\n",
								ped.PedidoId, ped.Estado, ped.RelojVectorial[0], ped.RelojVectorial[1], ped.RelojVectorial[2])
							file.WriteString(linea)
						}
					}

					file.WriteString("[AUDITORIA READ YOUR WRITES]\n")
					b, errRead := os.ReadFile("/app/output/RYW.log")
					if errRead == nil {
						file.Write(b)
					}
					file.WriteString("=================================\n")
					file.Close()
					log.Println("[Broker] Reporte.txt generado con éxito.")
				}

				log.Println("[Broker] Cierre del sistema por inactividad.")
				os.Exit(0)
			}
		}
	}()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error al abrir el puerto %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	pbBroker.RegisterBrokerServiceServer(grpcServer, &server{
		datanodeClients: clients,
		datanodeMap:     clientsMap,
		rrCounter:       0,
	})

	log.Println("[Broker] Listo y escuchando mensajes por gRPC.")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Error crítico al servir gRPC: %v", err)
	}
}
