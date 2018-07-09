This code is a set of runnable examples for the blog series: Fundamentals of data processing for SciFi geeks 

# Blog links 

* [Part 1](https://towardsdatascience.com/fundamentals-of-data-processing-part-i-f6a6914e1fec)
* [Part 2](https://towardsdatascience.com/fundamentals-of-data-processing-for-scifi-geeks-part-ii-apache-spark-rdd-3d4b2c6f39f)

# Setup to run the examples:

1. Install maven: https://maven.apache.org/install.html
2. Clone the repo.

Run the command from the repo directory:

3. Download "The Complete Works of William Shakespeare":

```wget -O ./data/shakespeare.txt http://www.gutenberg.org/files/100/100-0.txt```

4. Run the examples console:

```mvn -q clean install exec:java```

You will see the prompt:
```
Your wish is my command.
>>>
```
# Commands

You can run the following commands:
* iterator_topk
* iterator_word_count
* word_count_parallel
* word_count_serial
* word_count_rdd
* exit


