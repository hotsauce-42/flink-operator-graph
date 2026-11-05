package dev.flinkgraph.render;

import dev.flinkgraph.model.Edge;
import dev.flinkgraph.model.Node;
import dev.flinkgraph.model.NodeKind;
import dev.flinkgraph.model.OperatorGraph;
import org.eclipse.elk.alg.layered.options.LayeredOptions;
import org.eclipse.elk.core.RecursiveGraphLayoutEngine;
import org.eclipse.elk.core.options.CoreOptions;
import org.eclipse.elk.core.options.Direction;
import org.eclipse.elk.core.options.EdgeRouting;
import org.eclipse.elk.core.util.BasicProgressMonitor;
import org.eclipse.elk.graph.ElkEdge;
import org.eclipse.elk.graph.ElkEdgeSection;
import org.eclipse.elk.graph.ElkNode;
import org.eclipse.elk.graph.ElkBendPoint;
import org.eclipse.elk.graph.util.ElkGraphUtil;

import java.util.HashMap;
import java.util.Map;

/** Renders an uncompressed .drawio file; node positions and edge bend points come from ELK's layered layout. */
public final class DrawioRenderer {

    private DrawioRenderer() {}

    public static String render(OperatorGraph g) {
        ElkNode root = ElkGraphUtil.createGraph();
        root.setProperty(CoreOptions.ALGORITHM, LayeredOptions.ALGORITHM_ID);
        root.setProperty(CoreOptions.DIRECTION, Direction.RIGHT);
        root.setProperty(CoreOptions.EDGE_ROUTING, EdgeRouting.ORTHOGONAL);
        root.setProperty(LayeredOptions.SPACING_NODE_NODE_BETWEEN_LAYERS, 60.0);
        root.setProperty(CoreOptions.SPACING_NODE_NODE, 40.0);
        root.setProperty(CoreOptions.PADDING, new org.eclipse.elk.core.math.ElkPadding(20));

        Map<String, ElkNode> elkNodes = new HashMap<>();
        for (Node n : g.nodes()) {
            ElkNode en = ElkGraphUtil.createNode(root);
            double[] size = size(n);
            en.setDimensions(size[0], size[1]);
            elkNodes.put(n.id(), en);
        }
        Map<Edge, ElkEdge> elkEdges = new HashMap<>();
        for (Edge e : g.edges()) {
            elkEdges.put(e, ElkGraphUtil.createSimpleEdge(elkNodes.get(e.from()), elkNodes.get(e.to())));
        }
        new RecursiveGraphLayoutEngine().layout(root, new BasicProgressMonitor());

        StringBuilder sb = new StringBuilder();
        sb.append("<mxfile host=\"flink-operator-graph\">\n");
        sb.append("  <diagram name=\"").append(xml(g.name())).append("\" id=\"operator-graph\">\n");
        sb.append("    <mxGraphModel grid=\"1\" gridSize=\"10\" arrows=\"1\" connect=\"1\">\n");
        sb.append("      <root>\n        <mxCell id=\"0\"/>\n        <mxCell id=\"1\" parent=\"0\"/>\n");
        for (Node n : g.nodes()) {
            ElkNode en = elkNodes.get(n.id());
            sb.append("        <mxCell id=\"").append(n.id()).append("\" value=\"").append(xml(n.label()))
              .append("\" style=\"").append(style(n.kind())).append("\" vertex=\"1\" parent=\"1\">\n")
              .append("          <mxGeometry x=\"").append(num(en.getX())).append("\" y=\"").append(num(en.getY()))
              .append("\" width=\"").append(num(en.getWidth())).append("\" height=\"").append(num(en.getHeight()))
              .append("\" as=\"geometry\"/>\n        </mxCell>\n");
        }
        int i = 0;
        for (Edge e : g.edges()) {
            sb.append("        <mxCell id=\"e").append(++i).append("\" value=\"").append(xml(e.label() == null ? "" : e.label()))
              .append("\" style=\"edgeStyle=orthogonalEdgeStyle;rounded=1;html=0;endArrow=block;endFill=1;fontSize=10;labelBackgroundColor=#ffffff;\"")
              .append(" edge=\"1\" parent=\"1\" source=\"").append(e.from()).append("\" target=\"").append(e.to()).append("\">\n")
              .append("          <mxGeometry relative=\"1\" as=\"geometry\">\n");
            ElkEdge ee = elkEdges.get(e);
            if (!ee.getSections().isEmpty() && !ee.getSections().get(0).getBendPoints().isEmpty()) {
                ElkEdgeSection s = ee.getSections().get(0);
                sb.append("            <Array as=\"points\">\n");
                for (ElkBendPoint bp : s.getBendPoints()) {
                    sb.append("              <mxPoint x=\"").append(num(bp.getX())).append("\" y=\"").append(num(bp.getY())).append("\"/>\n");
                }
                sb.append("            </Array>\n");
            }
            sb.append("          </mxGeometry>\n        </mxCell>\n");
        }
        sb.append("      </root>\n    </mxGraphModel>\n  </diagram>\n</mxfile>\n");
        return sb.toString();
    }

    private static double[] size(Node n) {
        return switch (n.kind()) {
            case KEY_BY -> new double[] {90, 60};
            case UNION -> new double[] {60, 60};
            default -> {
                int longest = 0;
                for (String line : n.label().split("\n")) longest = Math.max(longest, line.length());
                yield new double[] {Math.max(120, longest * 7 + 24), 50};
            }
        };
    }

    private static String style(NodeKind kind) {
        String base = "whiteSpace=wrap;html=0;fontSize=11;";
        return base + switch (kind) {
            case SOURCE -> "rounded=1;arcSize=40;fillColor=#d5e8d4;strokeColor=#82b366;";
            case OPERATOR -> "rounded=1;arcSize=10;fillColor=#dae8fc;strokeColor=#6c8ebf;";
            case SINK -> "rounded=1;arcSize=40;fillColor=#ffe6cc;strokeColor=#d79b00;";
            case KEY_BY -> "rhombus;fillColor=#fff2cc;strokeColor=#d6b656;";
            case UNION -> "ellipse;fillColor=#f5f5f5;strokeColor=#666666;";
        };
    }

    private static String num(double d) {
        return String.valueOf(Math.round(d));
    }

    private static String xml(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("\n", "&#xa;");
    }
}
