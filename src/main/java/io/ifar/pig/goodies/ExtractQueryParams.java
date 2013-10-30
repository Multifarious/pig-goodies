package io.ifar.pig.goodies;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Pig UDF which takes an input String, parses as a URL query, and returns a Map of the key-value pairs.
 *
 * Input may be a whole URL or just the query part, optionally including leading "?". Trailing fragments are ignored.
 *
 * Query parameters with no "=value" are mapped to null.
 *
 * Query parameters with an "=" but no subsequent characters are mapped to empty String.
 *
 * There is syntactical ambiguity between a URL with no query parameters and a single query parameter whose value is null:
 * <ul>
 *    <li><code>http://this.is.a.url.with/no/query#fragment</code></li>
 *    <li><code>key_with_null_value</code></li>
 * </ul>
 *
 * The presence of an unencoded :, /, or # is used to infer a URL. Otherwise the value is treated as a bare query parameter.
 *
 * If a query parameter occurs multiple times, only the first occurrence is returned.
 *
 * The resulting Map is placed in a field with the same name as its input field. (Or, named "query" if no input schema available.)
 */
public class ExtractQueryParams extends EvalFunc<Map<String,String>> {
    private final String encoding;

    private static final Pattern DOUBLE_ENCODED_HEX = Pattern.compile("%25\\p{XDigit}{2}");

    private static final Pattern HTTP_OR_HTTPS = Pattern.compile("https?://");
    private final boolean shouldDetectDoubleEncoding;

    /**
     * Treat %xx input as UTF-8 encoded.
     */
    public ExtractQueryParams() {
        this("utf-8");
    }

    /**
     * Constructs ExtractQueryParams UDF.
     * @param encoding used when interpreting %xx input
     */
    public ExtractQueryParams(String encoding) {
        this(encoding,"false");
    }

    public ExtractQueryParams(String encoding, String shouldDetectDoubleEncoding) {
        // smoke check the encoding
        Charset.forName(encoding);
        this.encoding = encoding;
        this.shouldDetectDoubleEncoding = Boolean.valueOf(shouldDetectDoubleEncoding);
    }

    @Override
    public Map<String,String> exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        else if (input.size() > 1)
            throw new IOException("Expected single input String but got tuple of size " + input.size());

        String inputString = input.get(0).toString();
        if ("-".equals(inputString)) {
            return Collections.emptyMap();
        }

        if (shouldDetectDoubleEncoding && DOUBLE_ENCODED_HEX.matcher(inputString).find()) {
            inputString = URLDecoder.decode(inputString,encoding);
        }
        //trim anything preceding the query string and any # fragment identifiers afterwards
        int questIdx = inputString.indexOf('?');
        int hashIdx = inputString.indexOf('#');
        String trimmed = inputString.substring(questIdx + 1, hashIdx >= 0 ? hashIdx : inputString.length());

        Map<String,String> out = new HashMap<>();

        if (questIdx == -1 && HTTP_OR_HTTPS.matcher(inputString).find()) {
            return out;
        }
        String[] splits = trimmed.split("&");
        for (String split : splits) {
            String[] pieces = split.split("=",2);
            if (pieces.length > 1) {
                out.put(URLDecoder.decode(pieces[0],encoding),
                        URLDecoder.decode(pieces[1],encoding));
            } else {
                out.put(URLDecoder.decode(pieces[0],encoding),
                        null);
            }
        }
        return out;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            if (input == null) {
                return new Schema(new Schema.FieldSchema("query", null, DataType.MAP));
            } else {
                return new Schema(new Schema.FieldSchema(input.getField(0).alias, null, DataType.MAP));
            }
        } catch (FrontendException e) {
            return null;
        }
    }
}
