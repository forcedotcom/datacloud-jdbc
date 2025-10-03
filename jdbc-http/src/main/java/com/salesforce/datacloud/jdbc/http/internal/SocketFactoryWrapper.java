/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.http.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.Socket;
import javax.net.SocketFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Wrapper for Java's SocketFactory.
 *
 * It's main purpose is to avoid creating a SocksSocket when the socks proxy is disabled.
 */
@Slf4j
public class SocketFactoryWrapper extends SocketFactory {

    private final boolean isSocksProxyDisabled;
    private final SocketFactory socketFactory;

    public SocketFactoryWrapper(boolean isSocksProxyDisabled) {
        this(isSocksProxyDisabled, SocketFactory.getDefault());
    }

    public SocketFactoryWrapper(boolean isSocksProxyDisabled, SocketFactory socketFactory) {
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
