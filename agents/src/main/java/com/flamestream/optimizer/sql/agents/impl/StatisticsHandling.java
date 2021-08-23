package com.flamestream.optimizer.sql.agents.impl;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class StatisticsHandling {
    public static class StatsOutput extends PTransform<PCollection<Long>, PDone> {
        public StatsOutput(String tag) {
            this.tag = tag;
        }

        private String tag;

        @Override
        public PDone expand(PCollection<Long> input) {
            input.apply(ParDo.of(new DoFn<Long, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c, BoundedWindow window) {
                    MessageSender sender = new MessageSender(
                            tag + "-" + c.element().toString(),
                            new InetSocketAddress("localhost", 1111)
                    );
                    sender.send();
                }
            }));
            return PDone.in(input.getPipeline());
        }
    }

    public static class NIOServer implements Runnable {
        private final int port;

        public NIOServer(CoordinatorImpl coordinator, int port) {
            this.coordinator = coordinator;
            this.port = port;
        }

        public final CoordinatorImpl coordinator;
        public String result;

        @Override
        @SuppressWarnings("unused")
        public void run() {

            Selector selector;
            ServerSocketChannel serverSocket;
            InetSocketAddress address = new InetSocketAddress("localhost", port);
            try {
                selector = Selector.open();
                serverSocket = ServerSocketChannel.open();
                serverSocket.bind(address);
                serverSocket.configureBlocking(false);

                int ops = serverSocket.validOps();
                SelectionKey selectKy = serverSocket.register(selector, ops, null);

                for (;;) {
                    selector.select();

                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> selectedKeysIterator = selectedKeys.iterator();

                    while (selectedKeysIterator.hasNext()) {
                        SelectionKey key = selectedKeysIterator.next();
                        selectedKeysIterator.remove();

                        if (key.isAcceptable()) {
                            SocketChannel client = serverSocket.accept();

                            client.configureBlocking(false);
                            client.register(selector, SelectionKey.OP_READ);
                            log("Connection Accepted: " + client.getLocalAddress() + "\n");

                        } else if (key.isReadable()) {

                            SocketChannel client = (SocketChannel) key.channel();

                            ByteBuffer buffer = ByteBuffer.allocate(256);

                            if (client.read(buffer) < 0) {
                                key.cancel();
                                client.close();
                                continue;
                            }
                            result = new String(buffer.array()).trim();

                            log("Message received: " + result);
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void log(String str) {
            System.out.println(str);
        }
    }

    public static class MessageSender {
        private String messageToSend;
        private InetSocketAddress address;

        public MessageSender(String messageToSend, InetSocketAddress address) {
            this.messageToSend = messageToSend;
            this.address = address;
        }

        public void send() {
            try {
                SocketChannel client = SocketChannel.open(address);
                log("Connecting to Server on " + address);

                byte[] message = messageToSend.getBytes();
                ByteBuffer buffer = ByteBuffer.wrap(message);
                client.write(buffer);

                log("sending: " + messageToSend);
                buffer.clear();
                client.close();
            } catch (Exception e) {
                log("Exception: " + e);
                e.printStackTrace();
            }
        }

        private void log(String str) {
            System.out.println(str);
        }
    }
}
