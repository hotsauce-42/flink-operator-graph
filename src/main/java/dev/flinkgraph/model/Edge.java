package dev.flinkgraph.model;

/** {@code label} is null for plain forward edges. */
public record Edge(String from, String to, String label) {}
