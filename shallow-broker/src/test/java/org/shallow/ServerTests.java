package org.shallow;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ServerTests {

    @Test
    public void testTelnet() throws IOException {
        try (final Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", 7730), 100);
            boolean isConnected = socket.isConnected();

            Assert.assertTrue("Failed to telnet <127.0.0.1:7730>", isConnected);
        }
    }
}
