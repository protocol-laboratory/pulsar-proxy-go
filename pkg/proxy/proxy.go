// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package proxy

import (
	"errors"
	"github.com/protocol-laboratory/pulsar-codec-go/pb"
	"github.com/protocol-laboratory/pulsar-codec-go/pnet"
	"github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Config struct {
	Host                 string
	Port                 int
	ProxyVersion         string
	ProxyProtocolVersion int32
	ProxyMaxMessageSize  int32
}

type Server interface {
}

type ProxyServer struct {
	config        Config
	clientManager map[string]*pnet.PulsarNetClient
	clientLock    sync.RWMutex
	netServer     *pnet.PulsarNetServer

	server Server
}

func (p *ProxyServer) AcceptError(conn net.Conn, err error) {
	logrus.Errorf("accept %s connection error %v", conn.RemoteAddr(), err)
}

func (p *ProxyServer) ReadError(conn net.Conn, err error) {
	logrus.Errorf("read %s connection error %v", conn.RemoteAddr(), err)
}

func (p *ProxyServer) ReactError(conn net.Conn, err error) {
	logrus.Errorf("react %s connection error %v", conn.RemoteAddr(), err)
}

func (p *ProxyServer) WriteError(conn net.Conn, err error) {
	logrus.Errorf("write %s connection error %v", conn.RemoteAddr(), err)
}

func (p *ProxyServer) CommandConnect(conn net.Conn, connect *pb.CommandConnect) (*pb.CommandConnected, error) {
	if connect.ProxyToBrokerUrl == nil {
		logrus.Errorf("proxy to broker url is nil")
		return nil, errors.New("proxy to broker url is nil")
	}
	addr := conn.RemoteAddr().String()
	p.buildRpcClient(addr, *connect.ProxyToBrokerUrl)
	client, _ := p.getRpcClient(addr)
	resp, err := client.CommandConnect(connect)
	if err != nil {
		logrus.Errorf("send msg to broker failed, cause: %s", err)
	}
	return resp, err
}

func (p *ProxyServer) Close() {
}

func (p *ProxyServer) getRpcClient(addr string) (*pnet.PulsarNetClient, error) {
	p.clientLock.RLock()
	client, exist := p.clientManager[addr]
	p.clientLock.RUnlock()
	if !exist {
		return nil, errors.New("rpc client is not exist")
	}
	return client, nil
}

func (p *ProxyServer) buildRpcClient(addr, brokerUrl string) {
	p.clientLock.RLock()
	_, exist := p.clientManager[addr]
	p.clientLock.RUnlock()
	if !exist {
		p.clientLock.Lock()
		split := strings.Split(brokerUrl, ":")
		host := split[0]
		port, _ := strconv.Atoi(split[1])
		client, err := pnet.NewPulsarNetClient(pnet.PulsarNetClientConfig{
			Host:             host,
			Port:             port,
			BufferMax:        int(p.config.ProxyMaxMessageSize),
			SendQueueSize:    1024,
			PendingQueueSize: 1024,
		})
		if err != nil {
			logrus.Errorf("build rpc client failed, cause: %s", err)
		}
		p.clientManager[addr] = client
		p.clientLock.Unlock()
	}
}

func NewProxyServer(config *Config, server Server) (*ProxyServer, error) {
	p := &ProxyServer{}
	pulsarNetServer, err := pnet.NewPulsarNetServer(pnet.PulsarNetServerConfig{
		Host:      config.Host,
		Port:      config.Port,
		BufferMax: int(config.ProxyMaxMessageSize),
	}, p)
	if err != nil {
		return nil, err
	}
	p.netServer = pulsarNetServer
	p.server = server
	return p, nil
}
