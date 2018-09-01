package mapreduce.utility;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelGrepDataGenerator {
    public static String generate(Integer linesPerFile, Integer upperBound) throws FileNotFoundException {
        List<Integer> output = new ArrayList<>();
        Random random = new Random();
        IntStream.range(0, linesPerFile).forEach(j -> {
            output.add(random.nextInt(upperBound));
        });
        return output.stream().map(e -> e.toString()).collect(Collectors.joining("\n"));

    }

}
