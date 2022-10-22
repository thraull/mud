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

func NewP2PRelayClientDirect(config *relay.P2PRelayServerConfig, ethClient *ethclient.Client, logger *zap.Logger) *P2PRelayClientDirect {
	server := createP2PRelayServer(logger, ethClient, config)
	return &P2PRelayClientDirect{server: server}
}

func NewP2PRelayClientRemote(addr string, logger *zap.Logger) *P2PRelayClientRemote {
	// TODO: Set dial options
	conn, err := grpc.Dial(addr)
	if err != nil {
		logger.Info("error dialing remote p2p server", zap.Error(err))
		return nil
	}
	client := pb_relay.NewP2PRelayServiceClient(conn)
	return &P2PRelayClientRemote{client: client}
}

var _ pb_relay.P2PRelayServiceClient = (*P2PRelayClientRemote)(nil)
var _ pb_relay.P2PRelayServiceClient = (*P2PRelayClientDirect)(nil)

// **** Remote client ****

type P2PRelayClientRemote struct {
	client pb_relay.P2PRelayServiceClient
}

func (client *P2PRelayClientRemote) OpenStream(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (pb_relay.P2PRelayService_OpenStreamClient, error) {
	return client.client.OpenStream(ctx, in, opts...)
}

func (client *P2PRelayClientRemote) PushStream(ctx context.Context, opts ...grpc.CallOption) (pb_relay.P2PRelayService_PushStreamClient, error) {
	return client.client.PushStream(ctx, opts...)
}

// **** Direct client ****
// For every RPC, implement server and client-side streams and link them with a channel.

type P2PRelayClientDirect struct {
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

// Server-side stream for OpenStream (server -> client)

type P2POpenStreamStreamServer struct {
	scCh chan *PushRequestReply
	ctx  context.Context
	grpc.ServerStream
}

func (srv *P2POpenStreamStreamServer) Send(m *pb_relay.PushRequest) error {
	srv.scCh <- &PushRequestReply{r: m}
	return nil
}

func (srv *P2POpenStreamStreamServer) Context() context.Context { return srv.ctx }

func (srv *P2POpenStreamStreamServer) Err(err error) {
	if err == nil {
		return
	}
	srv.scCh <- &PushRequestReply{err: err}
}

// Client-side stream for OpenStream (server -> client)

type P2POpenStreamStreamClient struct {
	scCh chan *PushRequestReply
	ctx  context.Context
	grpc.ClientStream
}

func (cli *P2POpenStreamStreamClient) Recv() (*pb_relay.PushRequest, error) {
	m, ok := <-cli.scCh
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (cli *P2POpenStreamStreamClient) Context() context.Context { return cli.ctx }

func (cli *P2POpenStreamStreamClient) RecvMsg(anyMessage interface{}) error {
	m, err := cli.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*pb_relay.PushRequest)
	proto.Merge(outMessage, m)
	return nil
}

// Direct client for OpenStream

func (client *P2PRelayClientDirect) OpenStream(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (pb_relay.P2PRelayService_OpenStreamClient, error) {
	// 16384 = 2^14 (copied from Erigon hard-code).
	scCh := make(chan *PushRequestReply, 16384)
	streamServer := &P2POpenStreamStreamServer{scCh: scCh, ctx: ctx}
	go func() {
		defer close(scCh)
		streamServer.Err(client.server.OpenStream(in, streamServer))
	}()
	return &P2POpenStreamStreamClient{scCh: scCh, ctx: ctx}, nil
}

// PUSH_STREAM [ CLIENT -> SERVER ]

// Server-side stream of PushStream (client -> server)

type P2PPushStreamStreamServer struct {
	csCh chan *PushRequestReply
	scCh chan *EmptyReply
	ctx  context.Context
	grpc.ServerStream
}

func (srv *P2PPushStreamStreamServer) Recv() (*pb_relay.PushRequest, error) {
	m, ok := <-srv.csCh
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (srv *P2PPushStreamStreamServer) Context() context.Context { return srv.ctx }

func (srv *P2PPushStreamStreamServer) RecvMsg(anyMessage interface{}) error {
	m, err := srv.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*pb_relay.PushRequest)
	proto.Merge(outMessage, m)
	return nil
}

func (srv *P2PPushStreamStreamServer) SendAndClose(m *emptypb.Empty) error {
	srv.scCh <- &EmptyReply{r: m}
	return nil
}

// Client-side stream of PushStream (client -> server)

type P2PPushStreamStreamClient struct {
	csCh chan *PushRequestReply
	scCh chan *EmptyReply
	ctx  context.Context
	grpc.ClientStream
}

func (cli *P2PPushStreamStreamClient) Send(m *pb_relay.PushRequest) error {
	cli.csCh <- &PushRequestReply{r: m}
	return nil
}

func (cli *P2PPushStreamStreamClient) Context() context.Context { return cli.ctx }

func (cli *P2PPushStreamStreamClient) Err(err error) {
	if err == nil {
		return
	}
	cli.csCh <- &PushRequestReply{err: err}
}

func (cli *P2PPushStreamStreamClient) CloseAndRecv() (*emptypb.Empty, error) {
	cli.Err(io.EOF)
	m := <-cli.scCh
	return m.r, m.err
}

// Direct client for PushStream

func (client *P2PRelayClientDirect) PushStream(ctx context.Context, opts ...grpc.CallOption) (pb_relay.P2PRelayService_PushStreamClient, error) {
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
