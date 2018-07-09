package fundamentals.iterators;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;


public abstract class Iterator<I>{

    abstract boolean hasNext();

    abstract I next();

    /**
     * map function apply an operation to every element of the
     * iterator at the moment of iteration
     * @param mapper mapper function
     * @param <O> Returning type
     * @return new iterator over processed data
     */
    public <O> Iterator<O> map(Function<I, O> mapper) {

        Iterator<I> origin = this;

        return new Iterator<O>() {
            @Override
            boolean hasNext() {
                return origin.hasNext();
            }

            @Override
            O next() {
                return mapper.apply(origin.next());
            }
        };
    }

    /**
     * flatMap function iterates over
     * @param splitter splits every element of the iterator and returns inner iterator
     * @param <O> Returning type
     * @return new iterator over the split data
     */
    public <O> Iterator<O> flatMap(Function<I, Iterator<O>> splitter) {

        Iterator<I> origin = this;

        return new Iterator<O>() {

            //initialize current iterator as empty iterator
            Iterator<O> current = Iterators.empty();

            @Override
            boolean hasNext() {
                if (current.hasNext()){
                   return true;
                }
                if(origin.hasNext()){
                    current = splitter.apply(origin.next());
                    return hasNext();
                } else {
                    return false;
                }
            }

            @Override
            O next() {
                return current.next();
            }
        };
    }

    public Iterator<I> filter(Predicate<I> predicate){
        Iterator<I> origin = this;
        return new Iterator<I>() {

            I currentElement=null;

            @Override
            boolean hasNext() {
                if(Objects.nonNull(currentElement)) return true;
                do{
                    if(!origin.hasNext()) return false;
                    else {
                        currentElement=origin.next();
                    }
                }while (!predicate.test(currentElement));
                return true;
            }

            @Override
            I next() {
                if(hasNext()){
                    I rv = currentElement;
                    currentElement = null;
                    return rv;
                }
                else {
                    throw new NoSuchElementException("no elements satisfy the filter");
                }
            }
        };
    }

    public Iterator<Pair<I, Long>> zipWithIndex(){
        Iterator<I> origin = this;
        return new Iterator<Pair<I, Long>>() {

            Long currentIndex = 0l;

            @Override
            boolean hasNext() {
              return origin.hasNext();
            }

            @Override
            Pair<I, Long> next() {
              return new ImmutablePair<>(origin.next(), currentIndex++);
            }
        };
    };


    @FunctionalInterface
    public interface ThrowingConsumer<T, E extends Exception> {
        void accept(T t) throws E;
    }

    public <E extends Exception> void forEach(ThrowingConsumer<I, E> action) throws E{
        try {
            while (this.hasNext()) {
                action.accept(this.next());
            }
        }catch (Throwable ex){
            throw (E)ex;
        }
    }


    public <O> O reduce(O start, BiFunction<O, I, O> aggregator){
        O current = start;
        while(hasNext()){
            aggregator.apply(current, next());
        }
        return current;
    }

    public Iterator<I> take(int n){
        Iterator<I> origin = this;
        return new Iterator<I>() {
            int counter = 0;

            @Override
            boolean hasNext() {
                return origin.hasNext() && counter<n;
            }

            @Override
            I next() {
                counter++;
                return origin.next();
            }
        };
    }


    public List<I> collect(){
        return  this.reduce(new ArrayList<I>(), (agg, el)->{agg.add(el); return agg;});
    }

}






