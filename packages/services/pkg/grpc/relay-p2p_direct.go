package grpc

import (
	"context"
	"io"
	"latticexyz/mud/packages/services/pkg/relay"
	pb_relay "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func NewP2PClientDirect(config *relay.P2PRelayServerConfig, ethClient *ethclient.Client, logger *zap.Logger) *P2PClientDirect {
	server := createP2PRelayServer(logger, ethClient, config)
	return &P2PClientDirect{server: server}
}

func NewP2PClientRemote(addr string, logger *zap.Logger) *P2PClientRemote {
	// TODO: Set dial options
	conn, err := grpc.Dial(addr)
	if err != nil {
		logger.Info("error dialing remote p2p node", zap.Error(err))
		return nil
	}
	client := pb_relay.NewP2PRelayServiceClient(conn)
	return &P2PClientRemote{client: client}
}

var _ pb_relay.P2PRelayServiceClient = (*P2PClientRemote)(nil)
var _ pb_relay.P2PRelayServiceClient = (*P2PClientDirect)(nil)

// **** Remote client ****

type P2PClientRemote struct {
	client pb_relay.P2PRelayServiceClient
}

func (client *P2PClientRemote) OpenStream(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (pb_relay.P2PRelayService_OpenStreamClient, error) {
	return client.client.OpenStream(ctx, in, opts...)
}

func (client *P2PClientRemote) PushStream(ctx context.Context, opts ...grpc.CallOption) (pb_relay.P2PRelayService_PushStreamClient, error) {
	return client.client.PushStream(ctx, opts...)
}

// **** Direct client ****

type P2PClientDirect struct {
	server pb_relay.P2PRelayServiceServer
}

type PushRequestReply struct {
	r   *pb_relay.PushRequest
	err error
}

type EmptyReply struct {
	r   *emptypb.Empty
	err error
}

// OPEN_STREAM [ SERVER -> CLIENT ]

// Wrap the server-side stream for OpenStream (server -> client)

type P2POpenStreamStreamServer struct {
	ch  chan *PushRequestReply
	ctx context.Context
	grpc.ServerStream
}

func (s *P2POpenStreamStreamServer) Send(m *pb_relay.PushRequest) error {
	s.ch <- &PushRequestReply{r: m}
	return nil
}

func (s *P2POpenStreamStreamServer) Context() context.Context { return s.ctx }

func (s *P2POpenStreamStreamServer) Err(err error) {
	if err == nil {
		return
	}
	s.ch <- &PushRequestReply{err: err}
}

// Wrap the client-side stream for OpenStream (server -> client)

type P2POpenStreamStreamClient struct {
	ch  chan *PushRequestReply
	ctx context.Context
	grpc.ClientStream
}

func (c *P2POpenStreamStreamClient) Recv() (*pb_relay.PushRequest, error) {
	m, ok := <-c.ch
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (c *P2POpenStreamStreamClient) Context() context.Context { return c.ctx }

func (c *P2POpenStreamStreamClient) RecvMsg(anyMessage interface{}) error {
	m, err := c.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*pb_relay.PushRequest)
	proto.Merge(outMessage, m)
	return nil
}

// Implement direct client for OpenStream

func (client *P2PClientDirect) OpenStream(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (pb_relay.P2PRelayService_OpenStreamClient, error) {
	// 16384 = 2^14. This is what erigon has hard-coded.
	ch := make(chan *PushRequestReply, 16384)
	streamServer := &P2POpenStreamStreamServer{ch: ch, ctx: ctx}
	go func() {
		defer close(ch)
		streamServer.Err(client.server.OpenStream(in, streamServer))
	}()
	return &P2POpenStreamStreamClient{ch: ch, ctx: ctx}, nil
}

// PUSH_STREAM [ CLIENT -> SERVER ]

// Wrap the server-side stream of PushStream (client -> server)

type P2PPushStreamStreamServer struct {
	csCh chan *PushRequestReply
	scCh chan *EmptyReply
	ctx  context.Context
	grpc.ServerStream
}

func (s *P2PPushStreamStreamServer) Recv() (*pb_relay.PushRequest, error) {
	m, ok := <-s.csCh
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (s *P2PPushStreamStreamServer) Context() context.Context { return s.ctx }

func (s *P2PPushStreamStreamServer) RecvMsg(anyMessage interface{}) error {
	m, err := s.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*pb_relay.PushRequest)
	proto.Merge(outMessage, m)
	return nil
}

// TODO: reply generic empties by specific ones
func (s *P2PPushStreamStreamServer) SendAndClose(m *emptypb.Empty) error {
	s.scCh <- &EmptyReply{r: m}
	return nil
}

// Wrap the client-side stream of PushStream (client -> server)

type P2PPushStreamStreamClient struct {
	csCh chan *PushRequestReply
	scCh chan *EmptyReply
	ctx  context.Context
	grpc.ClientStream
}

func (c *P2PPushStreamStreamClient) Send(m *pb_relay.PushRequest) error {
	c.csCh <- &PushRequestReply{r: m}
	return nil
}

func (c *P2PPushStreamStreamClient) Context() context.Context { return c.ctx }

func (c *P2PPushStreamStreamClient) Err(err error) {
	if err == nil {
		return
	}
	c.csCh <- &PushRequestReply{err: err}
}

func (c *P2PPushStreamStreamClient) CloseAndRecv() (*emptypb.Empty, error) {
	c.Err(io.EOF)
	m := <-c.scCh
	return m.r, m.err
}

// Implement direct client for PushStream

func (client *P2PClientDirect) PushStream(ctx context.Context, opts ...grpc.CallOption) (pb_relay.P2PRelayService_PushStreamClient, error) {
	csCh := make(chan *PushRequestReply, 16384)
	scCh := make(chan *EmptyReply)
	streamServer := &P2PPushStreamStreamServer{csCh: csCh, scCh: scCh, ctx: ctx}
	clientServer := &P2PPushStreamStreamClient{csCh: csCh, scCh: scCh, ctx: ctx}
	go func() {
		defer close(csCh)
		defer close(scCh)
		client.server.PushStream(streamServer)
	}()
	return clientServer, nil
}
