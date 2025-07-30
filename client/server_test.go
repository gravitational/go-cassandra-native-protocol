// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCqlServer_Accept(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.NoError(t, err)

	clientConn1, err := clt1.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn1)

	clientConn2, err := clt2.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn2)

	serverConn1, err := server.Accept(clientConn1)
	require.NoError(t, err)
	require.NotNil(t, serverConn1)

	serverConn2, err := server.Accept(clientConn2)
	require.NoError(t, err)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_AcceptAny(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.NoError(t, err)

	clientConn1, err := clt1.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn1)

	serverConn1, err := server.AcceptAny()
	require.NoError(t, err)
	require.NotNil(t, serverConn1)

	clientConn2, err := clt2.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn2)

	serverConn2, err := server.AcceptAny()
	require.NoError(t, err)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_AllAcceptedClients(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.NoError(t, err)

	clientConn1, err := clt1.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn1)

	clientConn2, err := clt2.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn2)

	serverConn1, err := server.Accept(clientConn1)
	require.NoError(t, err)
	require.NotNil(t, serverConn1)

	serverConn2, err := server.Accept(clientConn2)
	require.NoError(t, err)
	require.NotNil(t, serverConn2)

	clients, err := server.AllAcceptedClients()
	require.NoError(t, err)
	require.NotNil(t, clients)
	require.Len(t, clients, 2)

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clients[0].IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clients[1].IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_Bind(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.NoError(t, err)

	clientConn1, serverConn1, err := server.Bind(clt1, ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn1)
	require.NotNil(t, serverConn1)

	clientConn2, serverConn2, err := server.Bind(clt2, ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn2)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_BindAndInit(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.NoError(t, err)

	clientConn1, serverConn1, err := server.BindAndInit(clt1, ctx, primitive.ProtocolVersion4, client.ManagedStreamId)
	require.NoError(t, err)
	require.NotNil(t, clientConn1)
	require.NotNil(t, serverConn1)

	clientConn2, serverConn2, err := server.Bind(clt2, ctx)
	require.NoError(t, err)
	require.NotNil(t, clientConn2)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

// TestCqlConnectionRace ensures there is no data race and invalid memory access
// during the server and client connections teardown.
//
// Run with this test with data race detector enabled.
// Example: `go test ./client/ -race -count=100 -run=TestCqlConnectionRace`
func TestCqlConnectionRace(t *testing.T) {
	srv := client.NewCqlServer("127.0.0.1:9043", nil)
	t.Cleanup(func() {
		srv.Close()
	})
	clt := client.NewCqlClient("127.0.0.1:9043", nil)

	require.NoError(t, srv.Start(t.Context()))

	// Connect to the server and start the incomingLoop and outgoingLoop.
	conn, srvConn, err := srv.BindAndInit(clt, t.Context(), primitive.ProtocolVersion4, client.ManagedStreamId)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		srvConn.Close()
	})

	// Perform a send to guarantee the loops are already running.
	_, err = conn.Send(frame.NewFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		&message.Query{
			Query:   "SELECT * FROM system.local",
			Options: &message.QueryOptions{},
		},
	))
	require.NoError(t, err)

	// Close the server connection, this will cause the client loops to abort
	// and clean up resources. This is where it will usually trigger a data race
	// as both connections still using some of the channels.
	require.NoError(t, srv.Close())
	require.NoError(t, srvConn.Close())
}
