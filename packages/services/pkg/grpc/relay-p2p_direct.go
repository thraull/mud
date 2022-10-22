package grpc

import (
	"context"
	"io"
	pb_relay "latticexyz/mud/packages/services/protobuf/go/ecs-relay"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ pb_relay.P2PRelayServiceClient = (*p2PRelayClientRemote)(nil)
var _ pb_relay.P2PRelayServiceClient = (*p2PRelayClientDirect)(nil)

// **** Remote client ****

type p2PRelayClientRemote struct {
	client pb_relay.P2PRelayServiceClient
}

func (client *p2PRelayClientRemote) OpenStream(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (pb_relay.P2PRelayService_OpenStreamClient, error) {
	return client.client.OpenStream(ctx, in, opts...)
}

func (client *p2PRelayClientRemote) PushStream(ctx context.Context, opts ...grpc.CallOption) (pb_relay.P2PRelayService_PushStreamClient, error) {
	return client.client.PushStream(ctx, opts...)
}

// **** Direct client ****
// For every RPC, implement server and client-side streams and link them with a channel.

type p2PRelayClientDirect struct {
	server pb_relay.P2PRelayServiceServer
}

type pushRequestReply struct {
	r   *pb_relay.PushRequest
	err error
}

type emptyReply struct {
	r   *emptypb.Empty
	err error
}

// OPEN_STREAM [ SERVER -> CLIENT ]

// Server-side stream for OpenStream (server -> client)

type p2POpenStreamStreamServer struct {
	scCh chan *pushRequestReply
	ctx  context.Context
	grpc.ServerStream
}

func (srv *p2POpenStreamStreamServer) Send(m *pb_relay.PushRequest) error {
	srv.scCh <- &pushRequestReply{r: m}
	return nil
}

func (srv *p2POpenStreamStreamServer) Context() context.Context { return srv.ctx }

func (srv *p2POpenStreamStreamServer) Err(err error) {
	if err == nil {
		return
	}
	srv.scCh <- &pushRequestReply{err: err}
	return
}

// Client-side stream for OpenStream (server -> client)

type p2POpenStreamStreamClient struct {
	scCh chan *pushRequestReply
	ctx  context.Context
	grpc.ClientStream
}

func (cli *p2POpenStreamStreamClient) Recv() (*pb_relay.PushRequest, error) {
	m, ok := <-cli.scCh
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (cli *p2POpenStreamStreamClient) Context() context.Context { return cli.ctx }

func (cli *p2POpenStreamStreamClient) RecvMsg(anyMessage interface{}) error {
	m, err := cli.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*pb_relay.PushRequest)
	proto.Merge(outMessage, m)
	return nil
}

// Direct client for OpenStream

func (client *p2PRelayClientDirect) OpenStream(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (pb_relay.P2PRelayService_OpenStreamClient, error) {
	// 16384 = 2^14 (copied from Erigon hard-code).
	scCh := make(chan *pushRequestReply, 16384)
	streamServer := &p2POpenStreamStreamServer{scCh: scCh, ctx: ctx}
	go func() {
		defer close(scCh)
		streamServer.Err(client.server.OpenStream(in, streamServer))
	}()
	return &p2POpenStreamStreamClient{scCh: scCh, ctx: ctx}, nil
}

// PUSH_STREAM [ CLIENT -> SERVER ]

// Server-side stream of PushStream (client -> server)

type p2PPushStreamStreamServer struct {
	csCh chan *pushRequestReply
	scCh chan *emptyReply
	ctx  context.Context
	grpc.ServerStream
}

func (srv *p2PPushStreamStreamServer) Recv() (*pb_relay.PushRequest, error) {
	m, ok := <-srv.csCh
	if !ok || m == nil {
		return nil, io.EOF
	}
	return m.r, m.err
}

func (srv *p2PPushStreamStreamServer) Context() context.Context { return srv.ctx }

func (srv *p2PPushStreamStreamServer) RecvMsg(anyMessage interface{}) error {
	m, err := srv.Recv()
	if err != nil {
		return err
	}
	outMessage := anyMessage.(*pb_relay.PushRequest)
	proto.Merge(outMessage, m)
	return nil
}

func (srv *p2PPushStreamStreamServer) SendAndClose(m *emptypb.Empty) error {
	srv.scCh <- &emptyReply{r: m}
	return nil
}

// Client-side stream of PushStream (client -> server)

type p2PPushStreamStreamClient struct {
	csCh chan *pushRequestReply
	scCh chan *emptyReply
	ctx  context.Context
	grpc.ClientStream
}

func (cli *p2PPushStreamStreamClient) Send(m *pb_relay.PushRequest) error {
	cli.csCh <- &pushRequestReply{r: m}
	return nil
}

func (cli *p2PPushStreamStreamClient) Context() context.Context { return cli.ctx }

func (cli *p2PPushStreamStreamClient) Err(err error) {
	if err == nil {
		return
	}
	cli.csCh <- &pushRequestReply{err: err}
	return
}

func (cli *p2PPushStreamStreamClient) CloseAndRecv() (*emptypb.Empty, error) {
	cli.Err(io.EOF)
	m := <-cli.scCh
	return m.r, m.err
}

// Direct client for PushStream

func (client *p2PRelayClientDirect) PushStream(ctx context.Context, opts ...grpc.CallOption) (pb_relay.P2PRelayService_PushStreamClient, error) {
	csCh := make(chan *pushRequestReply, 16384)
	scCh := make(chan *emptyReply)
	streamServer := &p2PPushStreamStreamServer{csCh: csCh, scCh: scCh, ctx: ctx}
	clientServer := &p2PPushStreamStreamClient{csCh: csCh, scCh: scCh, ctx: ctx}
	go func() {
		defer close(csCh)
		defer close(scCh)
		client.server.PushStream(streamServer)
	}()
	return clientServer, nil
}
