package dev.flinkgraph.model;

import java.util.List;

public record OperatorGraph(String name, List<Node> nodes, List<Edge> edges) {}
