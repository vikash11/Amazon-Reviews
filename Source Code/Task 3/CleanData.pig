REGISTER '/home/suresh/Desktop/AmazonReviews/CleanData/lib/CleanDataUDF.jar';
REGISTER '/home/suresh/Downloads/pig-0.14.0/lib/piggybank.jar';
-- REGISTER 's3://big-data-amazon-reviews/clean-data/lib/CleanDataUDF.jar';
-- REGISTER file:/usr/lib/pig/piggybank.jar

DEFINE MetaParser org.apache.pig.builtin.MetaParser();
DEFINE ReviewerParser org.apache.pig.builtin.ReviewerParser();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
SET default_parallel $clusters
-- Load	the meta data of products
reviewerData = LOAD '$review_file' USING TextLoader AS (line: chararray);
reviewerDataRows = FOREACH reviewerData GENERATE FLATTEN(ReviewerParser(line)) AS (reviewerID: chararray, asin: chararray, reviewerName: chararray, overall: float, helpful: int, overallCnt: int);

-- Load	the review data of products
metaData = LOAD '$meta_file' USING TextLoader AS (line: chararray);
metaDataRows = FOREACH metaData GENERATE FLATTEN(MetaParser(line)) AS (asin: chararray, category: chararray, brand: chararray);
metaDataRowsNotNulls = FILTER metaDataRows BY category is not null;

helpfulData = JOIN metaDataRowsNotNulls BY (asin), reviewerDataRows BY (asin);
helpfulDataColumnsFiltered = foreach helpfulData generate reviewerDataRows::asin,reviewerDataRows::reviewerID,reviewerDataRows::reviewerName,metaDataRowsNotNulls::category,metaDataRowsNotNulls::brand,reviewerDataRows::overall,reviewerDataRows::helpful,reviewerDataRows::overallCnt;


-- STORE reviewerDataRows INTO '/Users/gshivani/MapReduce/AmazonReviews/TopReviewers/op/opr';
-- STORE metaDataRows INTO '/Users/gshivani/MapReduce/AmazonReviews/TopReviewers/op/opm/';
-- STORE helpfulData INTO '/Users/gshivani/MapReduce/AmazonReviews/TopReviewers/op/helpful/';
STORE (foreach (group helpfulDataColumnsFiltered all) generate flatten($1)) INTO '$output_dir' USING CSVExcelStorage(',', 'NO_MULTILINE');;
