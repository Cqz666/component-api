import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RemoveTest {

    public static void main(String[] args) {
        List<String> timestamp = new ArrayList<>();
        timestamp.add("2");
        timestamp.add("3");
        timestamp.add("4");
        timestamp.add("0");
        timestamp.add("5");
        timestamp.add("1");
        timestamp.add("6");

        List<String> attributes = new ArrayList<>();
        attributes.add("b");
        attributes.add("c");
        attributes.add("d");
        attributes.add("x");
        attributes.add("e");
        attributes.add("a");
        attributes.add("f");

        Iterator<String> iterator = timestamp.iterator();
        Iterator<String> iterator1 = attributes.iterator();
        while (iterator.hasNext()&& iterator1.hasNext()){
            String value = iterator.next();
             iterator1.next();
            //假设超时时间是2
            if (Integer.parseInt(value)<4){
                iterator.remove();
                iterator1.remove();
            }
        }

        timestamp.forEach(System.out::println);
        attributes.forEach(System.out::println);

    }

}
