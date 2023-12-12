package org.meteor.listener;

import org.meteor.common.Node;

public interface ServerListener {
    void onStartup(Node node);

    void onShutdown(Node node);
}
