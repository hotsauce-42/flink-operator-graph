package dev.flinkgraph.cli;

import dev.flinkgraph.config.GraphsConfig;
import dev.flinkgraph.config.GraphsConfig.JobSpec;
import dev.flinkgraph.extract.StreamGraphExtractor;
import dev.flinkgraph.model.OperatorGraph;
import dev.flinkgraph.render.DrawioRenderer;
import dev.flinkgraph.render.MermaidRenderer;
import dev.flinkgraph.transform.GraphBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "flink-operator-graph", mixinStandardHelpOptions = true,
        description = "Generates unchained operator graphs (draw.io / Mermaid) for Flink DataStream jobs.")
public final class GenerateCommand implements Callable<Integer> {

    enum Format { drawio, mermaid }

    @Option(names = {"-c", "--config"}, required = true, description = "graphs.yaml listing the jobs")
    File config;

    @Option(names = {"-o", "--out"}, defaultValue = "graphs", description = "Output directory (default: ${DEFAULT-VALUE})")
    Path out;

    @Option(names = {"-f", "--format"}, split = ",", description = "Output formats: ${COMPLETION-CANDIDATES} (default: all)")
    EnumSet<Format> formats = EnumSet.allOf(Format.class);

    public static void main(String[] args) {
        System.exit(new CommandLine(new GenerateCommand()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        GraphsConfig cfg = GraphsConfig.load(config);
        File baseDir = config.getAbsoluteFile().getParentFile();
        Files.createDirectories(out);
        int failures = 0;
        for (JobSpec job : cfg.jobs()) {
            try {
                File jar = job.jar() == null ? null : resolve(baseDir, job.jar());
                var sg = StreamGraphExtractor.extract(jar, job.mainClass(),
                        job.args() == null ? List.of() : job.args(), cfg.parallelismFor(job));
                OperatorGraph graph = GraphBuilder.build(sg, cfg.optionsFor(job));
                if (formats.contains(Format.drawio)) {
                    Files.writeString(out.resolve(job.name() + ".drawio"), DrawioRenderer.render(graph));
                }
                if (formats.contains(Format.mermaid)) {
                    Files.writeString(out.resolve(job.name() + ".mmd"), MermaidRenderer.render(graph));
                }
                System.out.printf("OK    %s (%d nodes, %d edges)%n", job.name(), graph.nodes().size(), graph.edges().size());
            } catch (Exception e) {
                failures++;
                System.err.printf("FAIL  %s: %s%n", job.name(), e.getMessage());
            }
        }
        return failures == 0 ? 0 : 1;
    }

    private static File resolve(File baseDir, String path) {
        File f = new File(path);
        return f.isAbsolute() ? f : new File(baseDir, path);
    }
}
