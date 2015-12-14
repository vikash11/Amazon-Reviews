-- REGISTER '/home/suresh/Desktop/AmazonReviews/CategorywiseAggregation/lib/CategorywiseUDF.jar';
-- REGISTER '/home/suresh/Downloads/pig-0.14.0/lib/piggybank.jar';
REGISTER 's3://big-data-amazon-reviews/categorywise-aggre/lib/CategorywiseUDF.jar';
REGISTER file:/usr/lib/pig/piggybank.jar

DEFINE RecordParser org.apache.pig.builtin.RecordParser();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
SET default_parallel $clusters

-- Load	the clean data
categorywiseData = LOAD '$clean_data_file' USING TextLoader AS (line: chararray);
categorywiseDataRows = FOREACH categorywiseData GENERATE FLATTEN(RecordParser(line)) AS (category: chararray, overall: float, helpfulYes: float, helpfulCount: float);

categorywiseGroup = GROUP categorywiseDataRows BY category;

-- Aggregate function
categorywiseStat = FOREACH categorywiseGroup GENERATE group as category, AVG(categorywiseDataRows.overall), AVG(categorywiseDataRows.helpfulYes), AVG(categorywiseDataRows.helpfulCount), COUNT(categorywiseDataRows.category) AS COUNT;

STORE (foreach (group categorywiseStat all) generate flatten($1)) INTO '$output_dir' USING CSVExcelStorage(',', 'NO_MULTILINE');;
