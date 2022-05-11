package com.flamestream.optimizer.sql.agents.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;

public class NetworkUtil {
    private static final Logger LOG = LoggerFactory.getLogger("util.network");
    public static InetAddress getLocalAddress(InetSocketAddress addressToConnect) throws SocketException {
        try (final var datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(addressToConnect);
            LOG.info(datagramSocket.getInetAddress().toString());
            return datagramSocket.getInetAddress();
//            return datagramSocket.getLocalAddress();
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

    public static String getIPHost() {
        String ip = null;
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
        } catch (SocketException | UnknownHostException e) {
            LOG.error("can't obtain ip", e);
        }
        return ip;
    }
}
