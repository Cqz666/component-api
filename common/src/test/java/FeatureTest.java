import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class FeatureTest {
    public static void main(String[] args) {
        List<Long> timeList = Arrays.asList(
                97087981899L+1577808000000L
        );
        List<Long> sortedList = timeList.stream().sorted(Comparator.comparing(Long::longValue)).collect(Collectors.toList());
        sortedList.forEach(s-> System.out.println(s+"     "+format(s)));
    }
    private static String format(long timestamp){
        Date date = new Date(timestamp);
        SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMdd-HHmmss");
       return yyyyMMddHHmmss.format(date);
//        System.out.println(format);
    }
}
