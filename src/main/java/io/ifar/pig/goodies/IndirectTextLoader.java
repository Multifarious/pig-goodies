package io.ifar.pig.goodies;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.builtin.TextLoader;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * <p>An extension of the classic {@link TextLoader} that reads its input to extract a list of input locations.  This is
 * intended as a shim for environments where specifying a list of input paths directly as an input parameter is either
 * awkward or impossible.</p>
 */
public class IndirectTextLoader extends TextLoader{

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileSystem fs;
        try {
            fs = FileSystem.get(new URI(location), job.getConfiguration());
        } catch (URISyntaxException use) {
            throw new RuntimeException(use);
        }
        Path locationPath = new Path(location);
        FSDataInputStream stream = fs.open(locationPath);
        List<String> files = IOUtils.readLines(stream);
        // TextLoader#setLocation defers to FileInputFormat#setInputPaths(job,paths) where paths is a comma-separated
        // list of paths, so we comma-separate.
        super.setLocation(StringUtils.join(",",files),job);
    }
}
