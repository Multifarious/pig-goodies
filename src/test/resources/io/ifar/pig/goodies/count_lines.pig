-- trivial pig script to count lines
lines_in = LOAD '$INPUT' USING IndirectTextLoader AS (line:chararray);
all_lines = GROUP lines_in ALL;
count = FOREACH all_lines GENERATE COUNT(lines_in);