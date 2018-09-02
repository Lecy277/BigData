# BigData

These code segments use apache spark to solve various big data problems with python. The FrequentItemSets.py looks for the most frequently used item sets (like in the shopping cart problem) with a minimum support level from the Myfile.txt. The HTMLParser.py is a webcrawler I created to go through wikipedia pages and look for dead links known as "Red links" because they are pages that should have content but are blank. This is coupled with Project_BigData.py which was designed to go with the webcrawler and find not only the redlinks but also count and sort in order the most frequent links(secured or non secured), words, and numbers on each page. This program was able to run through 5,000 wikipedia pages on a laptop in 2:26 by modifying the code to run on 24 cpu partitions and using 8g of memory.
