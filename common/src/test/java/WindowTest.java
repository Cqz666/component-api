import org.apache.commons.math3.util.ArithmeticUtils;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class WindowTest {
    public static void main(String[] args) {
        long now = System.currentTimeMillis();
        long windowStartWithOffset = getWindowStartWithOffset(now, -Duration.ofHours(8).toMillis(), Duration.ofMinutes(1).toMillis());
        System.out.println(windowStartWithOffset);
        Date date = new Date(windowStartWithOffset);
        Date nowDate = new Date(now);
        SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = yyyyMMddHHmmss.format(date);
        String format1 = yyyyMMddHHmmss.format(nowDate);
//        System.out.println(format1);
        System.out.println("now_time:"+format1+"    window_start: "+format);

        long sliceSize = ArithmeticUtils.gcd(Duration.ofDays(7).toMillis(), Duration.ofMinutes(1).toMillis());     //大小7天，步长1分钟 的切片大小
        long seconds = Duration.ofMillis(sliceSize).getSeconds();
        System.out.println("slice scecond : "+seconds +"s");

        long numSlicesPerWindow = checkedDownCast(Duration.ofDays(7).toMillis() / sliceSize);

        System.out.println("numSlicesPerWindow : "+numSlicesPerWindow );

        long sliceSize2 = ArithmeticUtils.gcd(Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis());     //大小1小时，步长10分钟 的切片大小
        long seconds2 = Duration.ofMillis(sliceSize2).getSeconds();
        System.out.println("slice2 scecond : "+seconds2 +"s");

        System.out.println((3600*24*3));
        System.out.println(42531153+5667);
        System.out.println(31788225+24662);
    }

    public static int checkedDownCast(long value) {
        int downCast = (int) value;
        if (downCast != value) {
            throw new IllegalArgumentException(
                    "Cannot downcast long value " + value + " to integer.");
        }
        return downCast;
    }

    public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        final long remainder = (timestamp - offset) % windowSize;
        // handle both positive and negative cases
        if (remainder < 0) {
            return timestamp - (remainder + windowSize);
        } else {
            return timestamp - remainder;
        }
    }

}
