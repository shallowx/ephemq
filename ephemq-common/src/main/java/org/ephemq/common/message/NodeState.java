package org.ephemq.common.message;

/**
 * The NodeState class defines constants indicating the state of a node.
 * It is immutable and cannot be instantiated or extended.
 * <p>
 * The possible states are:
 * <p>
 * - NodeState.UP:     Indicates the node is in an operational state.
 * - NodeState.DOWN:   Indicates the node is in a non-operational state.
 */
public final class NodeState {
    public static final String UP = "UP";
    public static final String DOWN = "DOWN";
}
