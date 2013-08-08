DEFINE QueryParams io.ifar.pig.goodies.ExtractQueryParams();

data =
    LOAD 'input'
    AS (q:CHARARRAY);

udf_output =
  FOREACH data
  GENERATE
    QueryParams(q);

result =
  FOREACH udf_output
  GENERATE
    -- #'key' is Pig syntax for retrieving the named key from a Map value.
    -- ExtractQueryParams generates 1-tuples. The only field is named the same as the field that was parsed. ("q" in this case.)
    q#'foo', -- here we refer to the field by its name
    $0#'bar'; -- here we grab the first (and only) field from the tuple by index

STORE result INTO 'output';
