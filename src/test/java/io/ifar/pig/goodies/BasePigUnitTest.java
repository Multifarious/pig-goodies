package io.ifar.pig.goodies;

import org.apache.log4j.Logger;
import org.apache.pig.impl.PigContext;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Base class with some utility methods useful for PigUnit tests. In particular, it facilitates loading Pig scripts
 * and input data off the ClassLoader and placing items on the classpath.
 */
public abstract class BasePigUnitTest {

    protected abstract Logger getLog();

    /**
     * Packages to make available to Pig. E.g. "org.apache.pig.piggybank"
     */
    protected static List<String> getPackagesVisibleToPig() {
        return Collections.<String>emptyList();
    }

    @BeforeClass
    public static void beforeClass() {
        // ensure that our UDFs are visible to the Pig innards
        List<String> packages = new ArrayList<>(getPackagesVisibleToPig().size());
        for (String pkg : getPackagesVisibleToPig()) {
            packages.add(pkg.endsWith(".") ? pkg : pkg + '.');
        }

        Collections.addAll(PigContext.getPackageImportList(), packages.toArray(new String[packages.size()]));
    }

    protected String[] readLinesFromClassLoader(String resourceLocation) throws IOException {
        InputStream is = this.getClass().getResourceAsStream(resourceLocation);
        if (is == null) {
            String message = String.format("Unable to locate resource %s on the classpath.", resourceLocation);
            getLog().error(message);
            throw new RuntimeException(message);
        }
        if (resourceLocation.toLowerCase().endsWith(".gz")) {
            try {
                is = new GZIPInputStream(is);
            } catch (IOException ioe) {
                String message = String.format("Unable to wrap %s for decompression: (%s) %s", resourceLocation, ioe.getClass().getSimpleName(), ioe.getMessage());
                getLog().error(message);
                throw new RuntimeException(message,ioe);
            }
        }
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(is, Charset.forName("UTF-8")))) {
            List<String> lines = new ArrayList<>();
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                getLog().info(String.format("Read %d lines from %s.", reader.getLineNumber(), resourceLocation));
            } catch (IOException ioe) {
                String message = String.format("Unable to fully read %s due to %s: %s", resourceLocation,
                        ioe.getClass().getSimpleName(), ioe.getMessage());
                getLog().error(message);
                throw new RuntimeException(message,ioe);
            } finally {
                try {
                    reader.close();
                } catch (IOException ioe) {
                    // ignore
                }
            }
            return lines.toArray(new String[lines.size()]);
        }
    }
}
