Pig_FrequentItemsets
====================

 This code was my first dive into the PIG procedural programming language.  The pig language is highly extensible and can work well when wrapped in another code control language.  Its ability to handle a large variety of data structures makes it an easy choice for unstructured data.
 
 Ther pig code here is standalone pig code that parses apart a textfile.  It doesn't jsut do the typical workcount example but it insted performs associative analysis on the data to find out which words commonly appear together in documents.  
 
 This isn't a very useful example in its own right but the methodolgy behind it can be converted very easily to hadnle other types of itemset matching and market basket analyses.
 
 Future plans for this repo are for it to include code that allows it utilize the Apache Tika project code to parse the HTML code before it is tokenized by the sqoop processor.  Also adding code to wrap the itemset matching in and to perform pruning are additional features. These features are the secret sauce that make up the Apriori algorithm.
