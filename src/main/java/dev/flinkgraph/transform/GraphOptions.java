package dev.flinkgraph.transform;

/**
 * @param keyByAsNode   draw keyBy as its own node instead of only an edge label
 * @param collapseSinks merge Sink V2 "X: Writer" / "X: Committer" / "X: Global Committer" into one sink node
 */
public record GraphOptions(boolean keyByAsNode, boolean collapseSinks) {
    public static final GraphOptions DEFAULT = new GraphOptions(true, true);
}
