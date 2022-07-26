package org.shallow.topic;

import java.net.SocketAddress;
import java.util.List;

public class Partition {
    private long id;
    private String topic;
    private SocketAddress leader;
    private List<SocketAddress> replicas;
}
