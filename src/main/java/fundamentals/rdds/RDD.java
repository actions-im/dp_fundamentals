package fundamentals.rdds;

import fundamentals.iterators.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class RDD<T> {


    public ForkJoinPool getSparkContext() {
        return sparkContext;
    }

    public List<Dependency> getDependencies() {
        return dependencies;
    }

    ForkJoinPool sparkContext;
    List<Dependency> dependencies;

    public RDD(ForkJoinPool sparkContext, List<Dependency> dependencies) {
        this.sparkContext = sparkContext;
        this.dependencies = dependencies;
    }

    public RDD(RDD parent) {
        this.sparkContext = parent.getSparkContext();
        this.dependencies = Arrays.asList(new Dependency() {
            @Override
            public RDD rdd() {
                return parent;
            }
        });
    }

    public <P> RDD<P> getParent(){
        return (RDD<P>)this.dependencies.get(0).rdd();
    }


    protected abstract
    List<? extends Partition> getPartitions();

    protected abstract Iterator<T> compute(Partition partitionId);

    public <O> RDD<O> mapPartitions(Function<Iterator<T>, Iterator<O>> converter){
        return new MapPartitionsRDD<T, O>(this,converter);
    }

    static RDD<String> fromFile(Path directoryPath){
        return new PartitionedFileRDD(directoryPath);
    }

    <O> O aggregate(Supplier<O> start, BiFunction<O, T, O> partitionAggregator, BinaryOperator<O> combiner){

        RDD<T> origin = this;

        class PartitionHandler extends RecursiveTask<O>{

            Partition partition;

            public PartitionHandler(Partition partition) {
                this.partition = partition;
            }

            @Override
            protected O compute() {
                return origin.compute(partition).reduce(start.get(), partitionAggregator);
            }
        }

        RecursiveTask<O> aggregateTask = new RecursiveTask<O>() {
            @Override
            protected O compute() {
                List<ForkJoinTask<O>> runningTasks =
                    Iterators.fromJava(origin.getPartitions().iterator())
                        .map(p->new PartitionHandler(p).fork())
                        .collect();

                return Iterators.fromJava(runningTasks.iterator())
                        .map(ForkJoinTask::join)
                        .reduce(start.get(), combiner);
            }

        };

        return sparkContext.invoke(aggregateTask);

    }

    public <O> RDD<O> flatMap(Function<T, Iterator<O>> splitter){
        return this.mapPartitions(pi->pi.flatMap(splitter));
    }

    public <O> RDD<O> map(Function<T, O> mapper){
        return this.mapPartitions(pi->pi.map(mapper));
    }


}
