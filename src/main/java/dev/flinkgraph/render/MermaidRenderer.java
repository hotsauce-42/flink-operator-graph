package dev.flinkgraph.render;

import dev.flinkgraph.model.Edge;
import dev.flinkgraph.model.Node;
import dev.flinkgraph.model.OperatorGraph;

/** Renders a left-to-right Mermaid flowchart. Output is deterministic so diagrams diff cleanly in PRs. */
public final class MermaidRenderer {

    private MermaidRenderer() {}

    public static String render(OperatorGraph g) {
        StringBuilder sb = new StringBuilder();
        sb.append("---\ntitle: ").append(text(g.name())).append("\n---\n");
        sb.append("flowchart LR\n");
        for (Node n : g.nodes()) {
            String label = "\"" + text(n.label()) + "\"";
            String shape = switch (n.kind()) {
                case SOURCE -> "([" + label + "])";
                case SINK -> "[[" + label + "]]";
                case KEY_BY -> "{" + label + "}";
                case UNION -> "((" + label + "))";
                case OPERATOR -> "[" + label + "]";
            };
            sb.append("    ").append(n.id()).append(shape)
              .append(":::").append(n.kind().name().toLowerCase()).append('\n');
        }
        for (Edge e : g.edges()) {
            sb.append("    ").append(e.from());
            sb.append(e.label() == null ? " --> " : " -->|\"" + text(e.label()) + "\"| ");
            sb.append(e.to()).append('\n');
        }
        sb.append("""
                    classDef source fill:#d5e8d4,stroke:#82b366
                    classDef operator fill:#dae8fc,stroke:#6c8ebf
                    classDef sink fill:#ffe6cc,stroke:#d79b00
                    classDef key_by fill:#fff2cc,stroke:#d6b656
                    classDef union fill:#f5f5f5,stroke:#666666
                """);
        return sb.toString();
    }

    private static String text(String s) {
        return s.replace("\"", "#quot;").replace("\n", "<br/>");
    }
}
