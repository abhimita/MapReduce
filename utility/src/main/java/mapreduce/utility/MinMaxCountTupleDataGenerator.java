package mapreduce.utility;

import org.apache.commons.lang.RandomStringUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MinMaxCountTupleDataGenerator {
    public static List<String> generate() {
        List<String> users = Arrays.asList("John", "Sue", "Bob");
        List<LocalDate> dates = Arrays.asList(LocalDate.parse("20160101", DateTimeFormatter.ofPattern("yyyyMMdd")),
                LocalDate.parse("20160110", DateTimeFormatter.ofPattern("yyyyMMdd")));
        Random random = new Random();
        List<String> output = new ArrayList<String>();
        for (int i = 0; i < 1000; i++) {
            output.add(
                    users.get(random.nextInt(3)) + '\u0001' + dates.get(random.nextInt(2))
                            .plusDays(random.nextInt(365))
                            .atStartOfDay()
                            .plusSeconds(random.nextInt(86400))
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")) + '\u0001' + String.format("%s", RandomStringUtils.randomAlphanumeric(random.nextInt(50) + 1)));

        }
        return output;
    }
}

