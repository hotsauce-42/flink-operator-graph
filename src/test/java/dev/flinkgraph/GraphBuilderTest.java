package dev.flinkgraph;

import dev.flinkgraph.extract.StreamGraphExtractor;
import dev.flinkgraph.model.Node;
import dev.flinkgraph.model.NodeKind;
import dev.flinkgraph.model.OperatorGraph;
import dev.flinkgraph.render.DrawioRenderer;
import dev.flinkgraph.render.MermaidRenderer;
import dev.flinkgraph.samples.SampleJob;
import dev.flinkgraph.transform.GraphBuilder;
import dev.flinkgraph.transform.GraphOptions;
import org.junit.jupiter.api.Test;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphBuilderTest {

    /** Goes through the real PackagedProgram path, i.e. main() + execute() interception. */
    private static OperatorGraph sampleGraph(GraphOptions options) {
        var sg = StreamGraphExtractor.extract(null, SampleJob.class.getName(), List.of(), 2);
        return GraphBuilder.build(sg, options);
    }

    @Test
    void reconstructsSourcesSinksKeyByAndUnion() {
        OperatorGraph g = sampleGraph(GraphOptions.DEFAULT);
        System.out.println(MermaidRenderer.render(g));

        assertEquals("sample-job", g.name());
        assertEquals(List.of("Source: Orders", "Source: Returns"), names(g, NodeKind.SOURCE));
        assertEquals(List.of("Warehouse", "Dead Letter"), names(g, NodeKind.SINK));
        assertEquals(List.of("Validate", "Enrich"), names(g, NodeKind.OPERATOR));
        assertEquals(1, names(g, NodeKind.UNION).size());
        assertEquals(1, names(g, NodeKind.KEY_BY).size());
        assertTrue(g.edges().stream().anyMatch(e -> "side output: errors".equals(e.label())));
        assertTrue(g.edges().stream().anyMatch(e -> "REBALANCE".equals(e.label())));
    }

    @Test
    void keyByAsEdgeLabel() {
        OperatorGraph g = sampleGraph(new GraphOptions(false, true));
        assertTrue(names(g, NodeKind.KEY_BY).isEmpty());
        assertTrue(g.edges().stream().anyMatch(e -> "keyBy".equals(e.label())));
    }

    @Test
    void drawioIsWellFormedXmlWithAllCells() throws Exception {
        OperatorGraph g = sampleGraph(GraphOptions.DEFAULT);
        String xml = DrawioRenderer.render(g);
        var doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        // 2 root cells + nodes + edges
        assertEquals(2 + g.nodes().size() + g.edges().size(), doc.getElementsByTagName("mxCell").getLength());
    }

    @Test
    void collapsesSinkV2WriterAndCommitter() {
        var env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData("a").sinkTo(org.apache.flink.connector.file.sink.FileSink
                .forRowFormat(new org.apache.flink.core.fs.Path("/tmp/out"),
                        new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>())
                .build()).name("Files");

        OperatorGraph collapsed = GraphBuilder.build(env.getStreamGraph(false), GraphOptions.DEFAULT);
        assertEquals(List.of("Files"), names(collapsed, NodeKind.SINK));
        assertEquals(1, collapsed.edges().size());

        OperatorGraph raw = GraphBuilder.build(env.getStreamGraph(), new GraphOptions(true, false));
        assertTrue(raw.nodes().stream().anyMatch(n -> n.name().equals("Files: Committer")));
    }

    private static List<String> names(OperatorGraph g, NodeKind kind) {
        return g.nodes().stream().filter(n -> n.kind() == kind).map(Node::name).toList();
    }
}
