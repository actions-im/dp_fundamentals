package fundamentals.iterators;;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class IteratorExamples {

    public static PriorityQueue<Integer> topk(Integer[] data, int k){

        return Iterators.overArray(data)
                .reduce(new PriorityQueue<Integer>(k), (topn, el)->{
                    if(topn.size()<k){
                        topn.offer(el);
                    } else{
                        if(topn.peek()<el){
                            topn.poll();
                            topn.offer(el);
                        }
                    }
                    return topn;
                });

    }

    public static Map<String, Long> wordCount(File f) throws IOException{

        return Iterators.linesFromFile(f)
                .flatMap((line)->
                        //filter out meaningless lines
                    line.trim().length()>3 ?
                        //split the line into words
                        Iterators.overArray(line.split(" "))
                                .map(word->word.toLowerCase().trim())
                                .filter(word->!word.isEmpty())
                        :
                        Iterators.empty()
                )
                .reduce(new HashMap<>(), (agg, word)-> {
                            agg.put(word, agg.getOrDefault(word, 0l)+1);
                            return agg;
                        }
                );

    }

    public static void printWordCount(Map<String, Long> wordCount, int n){
        Iterators
            .fromJava(wordCount.entrySet().iterator())
            .map(entity-> entity.getKey()+" --> "+entity.getValue())
            .take(10)
            .forEach(line-> System.out.println(line));

        System.out.println("...");


    }



}