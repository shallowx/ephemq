package org.ostara.listener;

import org.ostara.common.Node;

public interface ServerListener {
    void onStartup(Node node);
    void onShutdown(Node node);
}
