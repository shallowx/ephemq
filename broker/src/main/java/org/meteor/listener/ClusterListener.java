package org.meteor.listener;

import org.meteor.common.Node;

public interface ClusterListener {
    void onGetControlRole(Node node);

    void onLostControlRole(Node node);

    void onNodeJoin(Node node);

    void onNodeDown(Node node);

    void onNodeLeave(Node node);
}
