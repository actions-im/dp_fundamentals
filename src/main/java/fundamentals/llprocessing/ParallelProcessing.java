package fundamentals.llprocessing;

import fundamentals.iterators.Iterator;
import fundamentals.iterators.IteratorExamples;
import fundamentals.iterators.Iterators;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class ParallelProcessing {


    private Path partitionPath;

    public ParallelProcessing(Path partitionPath) {
        this.partitionPath = partitionPath;
    }

    private class PartitionProcessingTask extends ForkJoinTask<Map<String, Long>> {

        int partitionId;

        public PartitionProcessingTask(int partitionId) {
            this.partitionId = partitionId;
        }

        Map<String, Long> result;

        @Override
        public Map<String, Long> getRawResult() {
            return result;
        }

        @Override
        protected void setRawResult(Map<String, Long> value) {
            result=value;
        }

        @Override
        protected boolean exec() {
            try {
                String partitionFileName = String.format("part-%1$s.txt",partitionId);
                File partition = partitionPath.resolve(partitionFileName).toFile();
                this.complete(IteratorExamples.wordCount(partition));
                System.out.println("Ready partition: "+ partitionId);
            }catch (IOException ex){
                this.completeExceptionally(ex);
            }
            return true;
        }
    };



    private class WordCountTask extends RecursiveTask<Map<String, Long>> {

        private List<ForkJoinTask<Map<String, Long>>> runningTasks;

        public WordCountTask(int numberOfPartitions) {
            runningTasks = Iterators.range(1, numberOfPartitions)
                    .map(partitionId -> new PartitionProcessingTask(partitionId))
                    .map(ForkJoinTask::fork)
                    .collect();
        }

        @Override
        protected Map<String, Long> compute() {
            return Iterators.fromJava(runningTasks.iterator())
                    .map(ForkJoinTask::join)
                    .reduce(new HashMap<>(), (agg, pwc)->{
                        pwc.forEach((word, partitionCount)->agg.put(word, agg.getOrDefault(word, 0L)+partitionCount));
                        return agg;
                    });
        }

    }

    private class SerialWordCountTask extends RecursiveTask<Map<String, Long>> {

        private Iterator<Map<String, Long>> runningTasks;

        public SerialWordCountTask(int numberOfPartitions) {
            runningTasks = Iterators.range(1, numberOfPartitions)
                    .map(partitionId -> new PartitionProcessingTask(partitionId))
                    .map(ForkJoinTask::fork)
                    .map(ForkJoinTask::join);

        }

        @Override
        protected Map<String, Long> compute() {
            return runningTasks
                    .reduce(new HashMap<>(), (agg, pwc)->{
                        pwc.forEach((word, partitionCount)->agg.put(word, agg.getOrDefault(word, 0L)+partitionCount));
                        return agg;
                    });
        }

    }

    public  Map<String, Long> wordCountSerial(int numberOfPartitions) {
        SerialWordCountTask wct = new SerialWordCountTask(numberOfPartitions);

        return ForkJoinPool.commonPool().invoke(wct);
    }

    public  Map<String, Long> wordCountParallel(int numberOfPartitions) {
        WordCountTask wct = new WordCountTask(numberOfPartitions);

        return ForkJoinPool.commonPool().invoke(wct);
    }

}
