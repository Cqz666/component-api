package com.cqz.component.common.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class JscksonTest {
    private static long l ;
    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        String json = "{\"client_timestamp\":1659577860017,\"cmd\":\"update\",\"fields\":{\"kind_id\":331756,\"dateline\":1659450054,\"subject\":\"天下武会\",\"display_order\":0,\"num_comment\":0,\"pid\":277156057,\"tid\":33861342,\"nick\":\"奇谋诸葛亮\",\"uid\":2316494274,\"num_view\":75,\"update_time\":0,\"seo_description\":\"\",\"post_type\":1,\"num_reply\":1,\"preview_pic\":\"\",\"paged\":1,\"tagid\":84126,\"src\":5,\"is_vip\":0,\"stype\":262144,\"terminal\":\"V1986A\",\"is_poll\":0,\"lastauthorid\":2316494274,\"lastpost\":1659453249,\"status\":105},\"from\":\"yhzx\",\"item_id\":33861342,\"item_type\":\"post\",\"server_timestamp\":1659577860017,\"signature\":\"e88d36e79d4fb6dc64b37ef1def4979b\",\"version_code\":\"1\"}";
        node.set("value", mapper.readValue(json.getBytes(), JsonNode.class));

        System.out.println(node.get("value").get("client_timestamp").asLong());

        int pos = 0;
        for (int i = ++pos; i < 5; i++) {
            System.out.println(i);
            pos++;
        }
        System.out.println(pos);
        System.out.println(++pos);
        System.out.println(++pos);

        System.out.println(l);

    }
}
