package fundamentals.rdds;

import fundamentals.iterators.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class PartitionedFileRDD extends RDD<String>{

    Path directoryPath;
    List<FilePartition> partitions;

    public PartitionedFileRDD(Path directoryPath) {
        super(ForkJoinPool.commonPool(), Collections.emptyList());
        this.directoryPath = directoryPath;
        this.partitions = Iterators
                .overArray(directoryPath.toFile().listFiles())
                .zipWithIndex()
                .map(indexedPartition->{
                    int index = indexedPartition.getRight().intValue();
                    File f = indexedPartition.getLeft();
                    return new FilePartition(index, f);
                })
                .collect();
    }

    @Override
    protected List<FilePartition> getPartitions() {
        return this.partitions;
    }

    @Override
    protected Iterator<String> compute(Partition partition){
        try{
            return Iterators.linesFromFile(partitions.get(partition.partitionId).file);
        }catch (IOException ex){
            // report failure
            return Iterators.empty();
        }
    }
}
