package io.ifar.pig.goodies;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Unit test for {@link IndirectTextLoader}.
 */
@RunWith(JUnit4.class)
public class TestIndirectTextLoader extends BasePigUnitTest {
    private static final Logger LOG = Logger.getLogger(TestIndirectTextLoader.class);

    private String fileOfPaths;

    @Override
    protected Logger getLog() {
        return LOG;
    }

    @Before
    public void before() throws IOException {
        File four = File.createTempFile(TestIndirectTextLoader.class.getSimpleName(),"four");
        four.deleteOnExit();
        File six = File.createTempFile(TestIndirectTextLoader.class.getSimpleName(),"six");
        six.deleteOnExit();
        IOUtils.copy(TestIndirectTextLoader.class.getResourceAsStream("four_lines.txt"),
                new FileOutputStream(four));
        IOUtils.copy(TestIndirectTextLoader.class.getResourceAsStream("six_lines.txt"),
                new FileOutputStream(six));
        File paths = File.createTempFile(TestIndirectTextLoader.class.getSimpleName(),"paths");
        paths.deleteOnExit();
        IOUtils.writeLines(ImmutableList.of(
                four.getAbsolutePath(),
                six.getAbsolutePath()
        ),"\n",new FileOutputStream(paths));
        fileOfPaths = paths.getAbsolutePath();
    }

    @Test
    public void combineTwoFiles() throws Exception {
        PigServer pig = null;
        try {
            Map<String,String> params = new HashMap<String,String>();
            params.put("INPUT",fileOfPaths);
            pig = PigServerUtils.prepPigServer(TestIndirectTextLoader.class.getResourceAsStream("count_lines.pig"),
                    ImmutableList.of(IndirectTextLoader.class.getPackage().getName()),
                    params);
            Iterator<Tuple> out = pig.openIterator("count");
            Assert.assertTrue(out.hasNext());
            Assert.assertEquals(10,Integer.parseInt(out.next().get(0).toString()));
        } catch (Exception e) {
            if (pig != null) {
                pig.shutdown();
            }
            throw Throwables.propagate(e);
        }
    }
}
