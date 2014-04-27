--%default INPUT_PATH 's3n://Cloud_Computing_Exploration/Html_files/Wikipedia_html' AS (line:chararray);

%default MIN_WORD_LENGTH '5'


DEFINE WORD_TOTALS(words_in, min_length)
RETURNS word_totals {
    

    -- Extract words from each line and put them into a pig bag
    -- datatype, then flatten the bag to get one word on each row
    words = FOREACH $words_in GENERATE FLATTEN(TOKENIZE(line)) AS word,Filename;


    -- filter out any words that are just white spaces
    filtered_words = FILTER words BY word MATCHES '\\w+';

    -- filter out any words that are just white spaces
    wordsUpper= FOREACH filtered_words GENERATE UPPER(word) AS word, Filename;
    -- trim
    trimmed_words = FOREACH wordsUpper GENERATE TRIM(word) AS word, Filename;


    significant_words   =   FILTER trimmed_words BY SIZE(word) >= $min_length;
    words_grouped       =   GROUP significant_words BY (Filename,word);
    $word_totals        =   FOREACH words_grouped GENERATE 
                                FLATTEN(group) as (Filename, word), 
                                COUNT(significant_words) AS count;
};

DEFINE WORD_TOTALS_FOR_ASSOCIATION(words)
RETURNS word_totals {
    words_grouped       =   GROUP $words BY word;
    $word_totals        =     FOREACH words_grouped {
                                Unique_Filename = DISTINCT $words.Filename;
                                GENERATE FLATTEN(group) as word, 
                                COUNT(Unique_Filename) AS word_appearance_count;
                                };
};

DEFINE File_TOTALS_FOR_ASSOCIATION(filenames)
RETURNS filecount_total {
    words_grouped       =   GROUP $filenames BY Filename;
    distinct_filecount        =     FOREACH words_grouped {
                                Unique_Filename = DISTINCT $filenames.Filename;
                                GENERATE COUNT(Unique_Filename) AS Filename_count;
                                };
    filecount_groups        =   GROUP distinct_filecount ALL;                          
    $filecount_total        =   FOREACH filecount_groups GENERATE COUNT(distinct_filecount.Filename_count) AS Filename_count;                            
};



/*
 * subset: {t: (word: chararray, occurrences: long, frequency: double)}
 * corpus: {t: (word: chararray, occurrences: long, frequency: double)}
 * min_corpus_frequency: double
 * ==>
 * rel_frequencies: {
 *                    t: (word: chararray, subset_occurrences: long, corpus_occurrences: long, 
 *                        subset_frequency: double, corpus_frequency: double, rel_frequency: double)
 *                  }
 */

DEFINE RemoveCommon(wordbank,commons)
    RETURNS filted_wordlist {
                    
                    joined_table = JOIN $commons BY commonword RIGHT, $wordbank BY word;
                    --words_grouped       =   GROUP joined_table ALL;
                    filtered_table = FILTER joined_table BY commonword is null;
                    $filted_wordlist = FOREACH filtered_table GENERATE Filename, word, count;
                                
                    };




DEFINE Apriori(wordcount_output)
    RETURNS confidence_support {

ParseUnCommonWords_main = FOREACH $wordcount_output GENERATE Filename, word as word;
ParseUnCommonWords_sub = FOREACH $wordcount_output GENERATE Filename as associative_filename, word as associative_word;

word_totals2       =   WORD_TOTALS_FOR_ASSOCIATION($wordcount_output);
file_totals2       =   File_TOTALS_FOR_ASSOCIATION($wordcount_output);

Joinedtotals = CROSS word_totals2, file_totals2;

word_frequencies       =   FOREACH Joinedtotals GENERATE word, word_appearance_count, (double)word_appearance_count / (double)Filename_count as support;

Joined_w_support = FOREACH (
    JOIN ParseUnCommonWords_main BY Filename, ParseUnCommonWords_sub BY associative_filename)
    GENERATE word,associative_word, associative_filename; 
--Joined_w_support_2 = FOREACH Joined_w_support GENERATE Filename, associative_filename,ParseUnCommonWords_main.word as word, ParseUnCommonWords_sub.associative_word as associative_word; 



association_join =     FOREACH (GROUP Joined_w_support by (word, associative_word)) GENERATE
                                FLATTEN(group) as (word, associative_word), 
                                COUNT(Joined_w_support.associative_filename) AS associative_count;

associationtotals = CROSS association_join, file_totals2;
frequency_analysis =   FOREACH associationtotals GENERATE word as mainword,associative_word,associative_count,  (double)associative_count / (double)Filename_count as itemset_support;
$confidence_support = FOREACH (
    JOIN frequency_analysis BY mainword, word_frequencies BY word) GENERATE mainword,associative_word,word_appearance_count as  main_word_appearance_count,support as mainword_support, associative_count, itemset_support as associative_word_support,(double)associative_count/(double)word_appearance_count  as confidence; 


};


        -- Top 500 Common English Words
        commonwords= LOAD 's3n://dgersonbucket/cloudcomputing/frequent_words.csv'   USING            org.apache.pig.piggybank.storage.CSVExcelStorage()
                    AS (commonword: chararray);

wiki_input_lines= LOAD 's3n://dgersonbucket/cloudcomputing/htmlfiles/wikipedia-articles/*.html' Using PigStorage('.','-tagFile') as (Filename:chararray,line:chararray);

wiki_word_totals       =   WORD_TOTALS(wiki_input_lines, $MIN_WORD_LENGTH);
wiki_ParseUnCommonWords = RemoveCommon(wiki_word_totals,commonwords);
wiki_confidence_and_support = Apriori(wiki_ParseUnCommonWords);

-- order the records by count
wiki_ordered_word_count = ORDER wiki_ParseUnCommonWords BY count DESC;
wiki_ordered_confidence_and_support = ORDER wiki_confidence_and_support BY confidence DESC;
STORE wiki_ordered_word_count INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/wiki/wordcounts.out';
STORE wiki_ordered_confidence_and_support INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/wiki/Confidence_and_support.out';


---------Azure Start-----------


azure_input_lines= LOAD 's3n://dgersonbucket/cloudcomputing/htmlfiles/azure-cases/*.html' Using PigStorage('.','-tagFile') as (Filename:chararray,line:chararray);

azure_word_totals       =   WORD_TOTALS(azure_input_lines, $MIN_WORD_LENGTH);
azure_ParseUnCommonWords = RemoveCommon(azure_word_totals,commonwords);
azure_confidence_and_support = Apriori(azure_ParseUnCommonWords);

-- order the records by count
azure_ordered_word_count = ORDER azure_ParseUnCommonWords BY count DESC;
azure_ordered_confidence_and_support = ORDER azure_confidence_and_support BY confidence DESC;
STORE azure_ordered_word_count INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/azure/wordcounts.out';
STORE azure_ordered_confidence_and_support INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/azure/Confidence_and_support.out';


---------Azure Start-----------


aws_input_lines= LOAD 's3n://dgersonbucket/cloudcomputing/htmlfiles/aws-cases/*.html' Using PigStorage('.','-tagFile') as (Filename:chararray,line:chararray);

aws_word_totals       =   WORD_TOTALS(aws_input_lines, $MIN_WORD_LENGTH);
aws_ParseUnCommonWords = RemoveCommon(aws_word_totals,commonwords);
aws_confidence_and_support = Apriori(aws_ParseUnCommonWords);

-- order the records by count
aws_ordered_word_count = ORDER aws_ParseUnCommonWords BY count DESC;
aws_ordered_confidence_and_support = ORDER aws_confidence_and_support BY confidence DESC;
STORE aws_ordered_word_count INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/aws/wordcounts.out';
STORE aws_ordered_confidence_and_support INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/aws/Confidence_and_support.out';



---------------Union------------
ALL_input_lines = UNION aws_input_lines, azure_input_lines , wiki_input_lines;


ALL_word_totals       =   WORD_TOTALS(ALL_input_lines, $MIN_WORD_LENGTH);
ALL_ParseUnCommonWords = RemoveCommon(ALL_word_totals,commonwords);
ALL_confidence_and_support = Apriori(ALL_ParseUnCommonWords);

-- order the records by count
ALL_ordered_word_count = ORDER ALL_ParseUnCommonWords BY count DESC;
ALL_ordered_confidence_and_support = ORDER ALL_confidence_and_support BY confidence DESC;
STORE ALL_ordered_word_count INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/ALL/wordcounts.out';
STORE ALL_ordered_confidence_and_support INTO 's3n://dgersonbucket/cloudcomputing/htmlfiles/Final_files/ALL/Confidence_and_support.out';