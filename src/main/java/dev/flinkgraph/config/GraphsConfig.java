package dev.flinkgraph.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.flinkgraph.transform.GraphOptions;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** The graphs.yaml file: defaults plus one entry per job. */
public record GraphsConfig(Defaults defaults, List<JobSpec> jobs) {

    public record Defaults(Boolean keyByAsNode, Boolean collapseSinks, Integer parallelism) {}

    /** {@code jar} is resolved relative to the config file. Null option fields fall back to the defaults. */
    public record JobSpec(String name, String jar, String mainClass, List<String> args,
                          Boolean keyByAsNode, Boolean collapseSinks, Integer parallelism) {}

    public static GraphsConfig load(File file) throws IOException {
        GraphsConfig c = new ObjectMapper(new YAMLFactory()).readValue(file, GraphsConfig.class);
        if (c.jobs() == null || c.jobs().isEmpty()) throw new IOException(file + ": no jobs configured");
        for (JobSpec j : c.jobs()) {
            if (j.name() == null || (j.jar() == null && j.mainClass() == null)) {
                throw new IOException(file + ": every job needs a name and a jar or mainClass");
            }
        }
        return c;
    }

    public GraphOptions optionsFor(JobSpec j) {
        Defaults d = defaults() == null ? new Defaults(null, null, null) : defaults();
        return new GraphOptions(
                first(j.keyByAsNode(), d.keyByAsNode(), GraphOptions.DEFAULT.keyByAsNode()),
                first(j.collapseSinks(), d.collapseSinks(), GraphOptions.DEFAULT.collapseSinks()));
    }

    public int parallelismFor(JobSpec j) {
        Integer d = defaults() == null ? null : defaults().parallelism();
        return first(j.parallelism(), d, 1);
    }

    private static <T> T first(T a, T b, T fallback) {
        return a != null ? a : b != null ? b : fallback;
    }
}
