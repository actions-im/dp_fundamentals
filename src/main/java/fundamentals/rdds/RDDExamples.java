package fundamentals.rdds;

import fundamentals.iterators.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public class RDDExamples {


    public static Map<String, Long> wordCount(Path path){

        RDD<String> relevantWordsRDD = RDD
            .fromFile(path)
            .flatMap(line->
                    //filter out meaningless lines
                    line.trim().length()>3 ?
                            //split the line into words
                            Iterators
                                .overArray(line.split(" "))
                                .map(word-> word.trim().toLowerCase())
                                .filter(word->!word.isEmpty())
                            :
                            Iterators.empty()
            );

        BiFunction<Map<String, Long>, String, Map<String, Long>> reducer = (agg, word)-> {
            agg.put(word, agg.getOrDefault(word, 0l)+1);
            return agg;
        };

        BinaryOperator<Map<String, Long>> combiner = (agg, pwc)->{
            pwc.forEach((word, partitionCount)->agg.put(word, agg.getOrDefault(word, 0L)+partitionCount));
            return agg;
        };


        Map<String, Long> wordCount = relevantWordsRDD.aggregate(HashMap::new, reducer, combiner);

        return wordCount;

    }


}
