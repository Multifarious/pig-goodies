package io.ifar.pig.goodies;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final Charset charset;


    /**
     * Treat %xx input as UTF-8 encoded.
     */
    public ExtractQueryParams() {
        this.charset = Charset.forName("UTF-8");
    }

    /**
     * Constructs ExtractQueryParams UDF.
     * @param encoding used when interpreting %xx input
     */
    public ExtractQueryParams(String encoding) {
        this.charset = Charset.forName(encoding);
    }

    @Override
    public Map<String,String> exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        else if (input.size() > 1)
            throw new IOException("Expected single input String but got tuple of size " + input.size());

        String inputString = input.get(0).toString();
        //trim anything preceding the query string and any # fragment identifiers afterwards
        int questIdx = inputString.indexOf('?');
        int hashIdx = inputString.indexOf('#');
        String trimmed = inputString.substring(questIdx + 1, hashIdx >= 0 ? hashIdx : inputString.length());

        List<NameValuePair> pairs = URLEncodedUtils.parse(trimmed, charset);
        if (pairs.size() == 1) {
            //distinguish between URL without query params and a bare key
            if (pairs.get(0).getValue() == null &&
                    (questIdx >= 1 || hashIdx >= 0 || inputString.indexOf('/') >= 0 || inputString.indexOf(':') >= 0) )
            {
                //Looks like a URL, not a bare key
                return new HashMap<>(0);
            }
        }
        Map<String,String> result = new HashMap<>(pairs.size());
        for (NameValuePair pair : pairs) {
            String existing = result.put(pair.getName(), pair.getValue());
            if (existing != null) {
                //oops, already had a value. Put it back. Handle this way rather than checking beforehand on assumption
                //that dupes are infrequent
                result.put(pair.getName(), existing);
            }
        }
        return result;
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
