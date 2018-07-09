package fundamentals.llprocessing;

import fundamentals.iterators.Iterator;
import fundamentals.iterators.Iterators;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class DataSplitting {

    public static void splitEvenly(Iterator<String> source, Path... destinations) throws IOException {

        int numberOfPartitions = destinations.length;

        BufferedWriter[] writers = new BufferedWriter[numberOfPartitions];

        Iterators.overArray(destinations)
                .zipWithIndex()
                .forEach(destIndexPair -> {
                            int writerIndex = destIndexPair.getRight().intValue();
                            Path currentPath = destIndexPair.getLeft();
                            writers[writerIndex] = Files.newBufferedWriter(currentPath,
                                    StandardOpenOption.CREATE, StandardOpenOption.APPEND
                            );
                        }
                );

        try {
            source
                    .zipWithIndex()
                    .forEach(indexedLine -> {
                        int currentPartition = (int) (indexedLine.getRight() % numberOfPartitions);
                        String currentLine = indexedLine.getLeft();
                        writers[currentPartition].write(currentLine+"\n");
                    });
        } finally {
            Iterators.overArray(writers).forEach(BufferedWriter::close);
        }
    }

    public static void splitByPages(Iterator<String> source, Path destination, int pageSize) throws IOException {

        source.zipWithIndex().forEach(
                new Iterator.ThrowingConsumer<Pair<String, Long>, IOException>() {
                    BufferedWriter currentWriter;
                    long currentPage = 0;

                    @Override
                    public void accept(Pair<String, Long> intexedLine) throws IOException {
                        long currentPosition = intexedLine.getRight();
                        String currentLine = intexedLine.getLeft();
                        long currentPage = currentPosition / pageSize;
                        if (currentPosition % pageSize == 0) {
                            if (currentWriter != null) currentWriter.close();
                            Path currentPath = Paths.get(destination.toString(), "part-" + currentPage + ".txt");
                            currentWriter = Files.newBufferedWriter(currentPath,
                                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }
                        currentWriter.write(currentLine+"\n");
                    }
                }
        );
    }

}
