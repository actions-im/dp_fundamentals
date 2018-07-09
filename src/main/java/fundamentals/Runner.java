package fundamentals;

import fundamentals.iterators.IteratorExamples;
import fundamentals.iterators.Iterators;
import fundamentals.llprocessing.DataSplitting;
import fundamentals.llprocessing.ParallelProcessing;
import fundamentals.rdds.RDDExamples;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.*;

public class Runner {

    private static final Path SHAKESPEARE_LOCATION = Paths.get(".","data", "shakespeare.txt");
    private static final Path SHAKESPEARE_DIR = SHAKESPEARE_LOCATION.getParent();
    private static final Path SHAKESPEARE_BY_PAGES = Paths.get(SHAKESPEARE_DIR.toString(), "/pages/");
    private static final Path SHAKESPEARE_PARTITIONED = Paths.get(SHAKESPEARE_DIR.toString(), "/partitioned/");
    private static int numberOfCores = Runtime.getRuntime().availableProcessors();


    private static void commandRouter(String command) throws IOException{


        if("iterator_topk".equalsIgnoreCase(command)){
            Integer[] data = {10,20,30,40,50,60,71,80,90,91};
            int k = 3;
            PriorityQueue<Integer> top = IteratorExamples.topk(data, k);

            System.out.println("Top k using iterators");
            for(int i:top){
                System.out.println(i);
            }
        }

        if("iterator_word_count".equalsIgnoreCase(command)){
            System.out.println("Word count");
            long before = System.currentTimeMillis();
            Map<String, Long> wordCount  = IteratorExamples.wordCount(SHAKESPEARE_LOCATION.toFile());
            System.out.println("Running time is:"+(System.currentTimeMillis()-before)+"ms.");
            IteratorExamples.printWordCount(wordCount, 10);
        }

        if("word_count_parallel".equalsIgnoreCase(command)) {
            long before = System.currentTimeMillis();
            new ParallelProcessing(SHAKESPEARE_PARTITIONED).wordCountParallel(numberOfCores);
            System.out.println("Running time is:"+(System.currentTimeMillis()-before)+"ms.");
        }

        if("word_count_serial".equalsIgnoreCase(command)) {
            long before = System.currentTimeMillis();
            new ParallelProcessing(SHAKESPEARE_PARTITIONED).wordCountSerial(numberOfCores);
            System.out.println("Running time is:"+(System.currentTimeMillis()-before)+"ms.");
        }

        if("word_count_rdd".equalsIgnoreCase(command)) {
            long before = System.currentTimeMillis();
            Map<String, Long> wordCount = RDDExamples.wordCount(SHAKESPEARE_PARTITIONED);
            System.out.println("Running time is:"+(System.currentTimeMillis()-before)+"ms.");
            IteratorExamples.printWordCount(wordCount, 10);
        }

        if("exit".equalsIgnoreCase(command)){
            System.exit(0);
        }

    }

    private static final void resetPath(Path directory) throws IOException{
        if(Files.exists(directory)) {
            Files.list(directory).forEach(p->{
                try{
                    Files.deleteIfExists(p);
                }catch (IOException ex){
                    System.out.println("Path does not exist: "+p.toString());
                }
            });
            Files.delete(directory);
        }
        Files.createDirectory(directory);
    }


    public static void main(String[] args) throws Exception{

        resetPath(SHAKESPEARE_PARTITIONED);
        resetPath(SHAKESPEARE_BY_PAGES);

        int cores = Runtime.getRuntime().availableProcessors();

        Path[] partitions = Iterators.range(1, cores)
                .map(partitionId -> SHAKESPEARE_PARTITIONED.resolve("part-"+partitionId+".txt"))
                .collect()
                .stream()
                .toArray(Path[]::new);

        DataSplitting.splitEvenly(
                Iterators.linesFromFile(SHAKESPEARE_LOCATION.toFile()),
                partitions
        );

        DataSplitting.splitByPages(
                Iterators.linesFromFile(SHAKESPEARE_LOCATION.toFile()), SHAKESPEARE_BY_PAGES, 1000
        );

        System.out.println("Your wish is my command.");

        Scanner scanner = new Scanner(System.in);
        while(true){
            System.out.print(">>>");
            String command = scanner.nextLine();
            commandRouter(command);
        }


    }

}
