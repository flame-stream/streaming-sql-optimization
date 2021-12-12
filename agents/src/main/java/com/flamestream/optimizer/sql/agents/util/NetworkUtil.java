package com.flamestream.optimizer.sql.agents.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;

public class NetworkUtil {
    private static final Logger LOG = LoggerFactory.getLogger("util.network");
    public static InetAddress getLocalAddress(InetSocketAddress addressToConnect) throws SocketException {
        try (final var datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(addressToConnect);
            return datagramSocket.getLocalAddress();
        }
    }

    public static String getLocalAddressHost(InetSocketAddress address) {
        String host;
        try {
            InetAddress localAddress = getLocalAddress(address);
            host = localAddress.getHostAddress();
        } catch (SocketException e) {
            LOG.error("unable to determine local address", e);
            host = "localhost";
        }
        return host;
    }
}
