package io.ifar.pig.goodies;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.builtin.TextLoader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>An extension of the classic {@link TextLoader} that reads its input to extract a list of input locations.  This is
 * intended as a shim for environments where specifying a list of input paths directly as an input parameter is either
 * awkward or impossible.</p>
 */
public class IndirectTextLoader extends TextLoader{

    private static final Log LOG = LogFactory.getLog(IndirectTextLoader.class);

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileSystem fs;
        try {
            fs = FileSystem.get(new URI(location), job.getConfiguration());
        } catch (URISyntaxException use) {
            LOG.error(String.format("Unable to parse location %s as a URI: (%s) %s",
                    location, use.getClass().getSimpleName(), use.getMessage()));
            throw new RuntimeException(use);
        }
        Path locationPath = new Path(location);
        FSDataInputStream stream = fs.open(locationPath);
        LineNumberReader r = new LineNumberReader(new InputStreamReader(stream, Charset.forName("utf-8")));
        List<String> files = new ArrayList<String>();
        String line;
        while ((line = r.readLine()) != null) {
            LOG.info(String.format("Adding %s to the list of locations to process.",line));
            files.add(line);
        }
        // TextLoader#setLocation defers to FileInputFormat#setInputPaths(job,paths) where paths is a comma-separated
        // list of paths, so we comma-separate.
        super.setLocation(StringUtils.join(",",files),job);
    }
}