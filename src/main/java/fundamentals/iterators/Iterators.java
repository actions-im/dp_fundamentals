package fundamentals.iterators;;


import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.*;
import java.util.NoSuchElementException;

public class Iterators {

    public static <O> Iterator<O> empty() {
        return new Iterator<O>() {
            @Override
            boolean hasNext() {
                return false;
            }

            @Override
            O next() {
                throw new NoSuchElementException("called next on empty operator");
            }
        };
    }


    /**
     * Returns an iterator over the lines in the file
     *
     * @param input - Input file
     * @return iterators.Iterator over the elements of array
     */
    public static <T> Iterator<T> overArray(T[] input) {
        return new Iterator<T>() {

            int currentIndex = 0;

            @Override
            boolean hasNext() {
                return currentIndex < input.length;
            }

            @Override
            T next() {
                return input[currentIndex++];
            }
        };

    }

    public static Iterator<Integer> range(Integer from, Integer to){
        return new Iterator<Integer>() {

            int currentIndex = from;

            @Override
            boolean hasNext() {
                return currentIndex <= to;
            }

            @Override
            Integer next() {
                return currentIndex++;
            }
        };
    }

    /**
     * Returns an iterator over the lines in the file
     *
     * @param f - Input file
     * @return iterators.Iterator over lines of the file
     * @throws IOException
     */
    public static Iterator<String> linesFromFile(File f) throws IOException {
        return new Iterator<String>() {
            BufferedReader reader = Files.newBufferedReader(f.toPath());
            String nextLine = null;

            @Override
            public boolean hasNext() {
                if (nextLine != null) {
                    return true;
                } else {
                    try {
                        nextLine = reader.readLine();
                        return (nextLine != null);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }

            @Override
            public String next() {
                if (nextLine != null || hasNext()) {
                    String line = nextLine;
                    nextLine = null;
                    return line;
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    public static <I> Iterator<I> fromJava(java.util.Iterator<I> inner){
            return new Iterator<I>(){
                @Override
                boolean hasNext() {
                    return inner.hasNext();
                }

                @Override
                I next() {
                    return inner.next();
                }
            };
    }


}