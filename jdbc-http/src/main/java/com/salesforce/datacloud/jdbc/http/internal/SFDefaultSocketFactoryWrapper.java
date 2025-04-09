/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.http.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.Socket;
import javax.net.SocketFactory;
import lombok.extern.slf4j.Slf4j;

/** Default Wrapper for SocketFactory. */
@Slf4j
public class SFDefaultSocketFactoryWrapper extends SocketFactory {

    private final boolean isSocksProxyDisabled;
    private final SocketFactory socketFactory;

    public SFDefaultSocketFactoryWrapper(boolean isSocksProxyDisabled) {
        this(isSocksProxyDisabled, SocketFactory.getDefault());
    }

    public SFDefaultSocketFactoryWrapper(boolean isSocksProxyDisabled, SocketFactory socketFactory) {
        super();
        this.isSocksProxyDisabled = isSocksProxyDisabled;
        this.socketFactory = socketFactory;
    }

    /**
     * When <code>isSocksProxyDisabled</code> then, socket backed by plain socket impl is returned. Otherwise, delegates
     * the socket creation to specified socketFactory
     *
     * @return socket
     * @throws IOException when socket creation fails
     */
    @Override
    public Socket createSocket() throws IOException {
        // avoid creating SocksSocket when SocksProxyDisabled
        // this is the method called by okhttp
        return isSocksProxyDisabled ? new Socket(Proxy.NO_PROXY) : this.socketFactory.createSocket();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return this.socketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(InetAddress address, int port) throws IOException {
        return this.socketFactory.createSocket(address, port);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort) throws IOException {
        return this.socketFactory.createSocket(host, port, clientAddress, clientPort);
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress clientAddress, int clientPort)
            throws IOException {
        return this.socketFactory.createSocket(address, port, clientAddress, clientPort);
    }
}
