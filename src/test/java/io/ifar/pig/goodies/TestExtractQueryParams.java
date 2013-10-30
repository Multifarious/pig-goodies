package io.ifar.pig.goodies;

import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Logger;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestExtractQueryParams extends BasePigUnitTest {
    private static final Logger LOG = Logger.getLogger(TestExtractQueryParams.class);

    private ExtractQueryParams f;
    private TupleFactory tupleFactory;
    private ExtractQueryParams g;

    @Before
    public void setup() {
        f = new ExtractQueryParams("UTF-8");
        g = new ExtractQueryParams("UTF-8","true");
        tupleFactory = TupleFactory.getInstance();
    }

    @Test
    public void testNullInput() throws Exception {
        assertNull(f.exec(null));
    }

    @Test
    public void testEmptyInput() throws Exception {
        assertNull(f.exec(tupleFactory.newTuple(0)));
    }

    @Test(expected = IOException.class)
    public void testArityTooGreat() throws IOException {
        f.exec(tupleFactory.newTuple(2));
    }

    @Test
    public void testSingleParam() throws Exception {
        assertEquals(
                ImmutableMap.of("foo","baz"),
                f.exec(tupleFactory.newTuple("?foo=baz")));
    }

    @Test
    public void testMultipleParams() throws Exception {
        assertEquals(
                ImmutableMap.of("foo","baz","bar","42"),
                f.exec(tupleFactory.newTuple("?foo=baz&bar=42")));
    }

    @Test
    public void testNullValuedParams() throws Exception {
        Map<String,String> expected = new HashMap<>(2);
        expected.put("foo", null);
        expected.put("bar", "");
        assertEquals(
                expected,
                f.exec(tupleFactory.newTuple("?foo&bar=")));
    }

    @Test
    public void testSingleNullValue() throws Exception {
        Map<String,String> expected = new HashMap<>(2);
        expected.put("foo", null);
        assertEquals(
                expected,
                f.exec(tupleFactory.newTuple("foo")));
    }

    @Test
    public void testReturnLast() throws Exception {
        assertEquals(
                ImmutableMap.of("foo","42"),
                f.exec(tupleFactory.newTuple("?foo=baz&foo=42")));
    }

    @Test
    public void testUrlDecoding() throws Exception {
        assertEquals(
                ImmutableMap.of("foo[]? bar","baz=baz&baz"),
                f.exec(tupleFactory.newTuple("?foo%5B%5D%3F%20bar=baz%3Dbaz%26baz")));
    }

    @Test
    public void testFullUrl() throws Exception {
        assertEquals(
                ImmutableMap.of("q", "example urls"),
                f.exec(tupleFactory.newTuple("https://duckduckgo.com/?q=example+urls"))
        );
    }

    @Test
    public void testIgnoreFragments() throws Exception {
        assertEquals(
                ImmutableMap.of("q", "example urls"),
                f.exec(tupleFactory.newTuple("https://duckduckgo.com/?q=example+urls#fragment"))
        );
    }

    @Test
    public void testFullUrlWithNoQueryParams() throws Exception {
        assertEquals(
                ImmutableMap.<String,String>of(),
                f.exec(tupleFactory.newTuple("https://en.wikipedia.org/wiki/Uniform_Resource_Locator#Syntax"))
        );
    }

    @Test
    public void testCharSets() throws Exception {
        assertEquals(
                ImmutableMap.<String,String>of("\u03B1","\u03B2"), //alpha, beta
                f.exec(tupleFactory.newTuple("?%CE%B1=%CE%B2")) //see http://www.utf8-chartable.de/unicode-utf8-table.pl?start=768
        );
        ExtractQueryParams fGreek = new ExtractQueryParams("ISO-8859-7");
        assertEquals(
                ImmutableMap.<String,String>of("\u03B1","\u03B2"), //alpha, beta
                fGreek.exec(tupleFactory.newTuple("?%E1=%E2")) //see https://en.wikipedia.org/wiki/ISO/IEC_8859-7
        );
    }

    @Test
    public void testDoubleEncoded() throws Exception {
        assertEquals(
                ImmutableMap.of("foo[]? bar", "baz=baz&baz"),
                g.exec(tupleFactory.newTuple("?foo%255B%255D%253F%2520bar=baz%253Dbaz%2526baz")));
    }

    @Override
    protected Logger getLog() {
        return LOG;
    }

    @Test
    public void testExtractSingleQueryParameter() throws Exception {
        String[] args = {};

        PigTest test = new PigTest(readLinesFromClassLoader("extract_query_parameter.pig"), args);

        String[] input = {
                "?foo=baz",
                "?bar=42&foo=99"
        };

        String[] output = {
                "(baz,)",
                "(99,42)"
        };

        test.assertOutput("data", input, "result", output);
        //alternately: test.assertOutput("data",readLinesFromClassloader("input.log.gz"), "result",readLinesFromClassloader("expected.out.gz"));
    }
}