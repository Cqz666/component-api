import com.cqz.component.flink.job.cep.condition.EndCondition;
import com.cqz.component.flink.job.cep.condition.StartCondition;
import com.cqz.component.flink.job.cep.event.Event;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

public class CepJsonUtilsTest {

    @Test
    public void patternToJson() throws JsonProcessingException {
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new StartCondition("action == 0"))
                .timesOrMore(3)
                .followedBy("end")
                .where(new EndCondition());
        String patternToJSONString = CepJsonUtils.convertPatternToJSONString(pattern);
        System.out.println(patternToJSONString);
    }

}
