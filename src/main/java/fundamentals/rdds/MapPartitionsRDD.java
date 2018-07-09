package fundamentals.rdds;

import fundamentals.iterators.*;

import java.util.List;
import java.util.function.Function;

public class MapPartitionsRDD<T,O> extends RDD<O> {

    Function<Iterator<T>, Iterator<O>> converter;

    public MapPartitionsRDD(RDD parent, Function<Iterator<T>, Iterator<O>> converter) {
        super(parent);
        this.converter = converter;
    }

    @Override
    protected List<? extends Partition> getPartitions() {
        return getParent().getPartitions();
    }

    @Override
    protected Iterator<O> compute(Partition partitionId) {
        Iterator<T> preComputed = (Iterator<T>)getParent().compute(partitionId);
        return converter
                .apply(preComputed);
    }
}
