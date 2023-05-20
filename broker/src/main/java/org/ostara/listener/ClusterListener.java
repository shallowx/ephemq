package org.ostara.listener;

import org.ostara.common.Node;

public interface ClusterListener {
    void onGetControlRole(Node node);
    void onLostControlRole(Node node);
    void onNodeJoin(Node node);
    void onNodeDown(Node node);
    void onNodeLeave(Node node);
}
