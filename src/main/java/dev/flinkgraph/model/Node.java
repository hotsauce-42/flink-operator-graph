package dev.flinkgraph.model;

/** A box in the diagram. {@code parallelism} is 0 for virtual nodes (keyBy, union). */
public record Node(String id, NodeKind kind, String name, int parallelism) {

    public String label() {
        return parallelism > 0 ? name + "\nParallelism: " + parallelism : name;
    }
}
