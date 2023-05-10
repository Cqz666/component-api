package com.cqz.component.flink.job.cep.dynamic;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.dynamic.impl.json.deserializer.ConditionSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.NodeSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.TimeStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class JDBCPeriodicPatternProcessorDiscoverer<T> extends PeriodicPatternProcessorDiscoverer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCPeriodicPatternProcessorDiscoverer.class);

    private final String tableName;
    private final List<PatternProcessor<T>> initialPatternProcessors;
    private final ClassLoader userCodeClassLoader;
    private Statement statement;
    private ResultSet resultSet;
    private Map<String, Tuple4<String, Integer, String, String>> latestPatternProcessors;

    public JDBCPeriodicPatternProcessorDiscoverer(
            final String jdbcUrl,
            final String jdbcDriver,
            final String tableName,
            final ClassLoader userCodeClassLoader,
            final List<PatternProcessor<T>> initialPatternProcessors,
            final Long intervalMillis) throws Exception {
        super(intervalMillis);
        this.tableName = requireNonNull(tableName);
        this.initialPatternProcessors = initialPatternProcessors;
        this.userCodeClassLoader = userCodeClassLoader;

        Class.forName(requireNonNull(jdbcDriver));
        this.statement = DriverManager.getConnection(requireNonNull(jdbcUrl)).createStatement();
    }

    @Override
    public boolean arePatternProcessorsUpdated() {
        if (latestPatternProcessors == null && !CollectionUtil.isNullOrEmpty(initialPatternProcessors)) {
            return true;
        }
        if (statement == null) {
            return false;
        }
        try {
            resultSet = statement.executeQuery("SELECT * FROM " + tableName);
            Map<String, Tuple4<String, Integer, String, String>> currentPatternProcessors = new HashMap<>();
            while (resultSet.next()) {
                String id = resultSet.getString("id");
                if (currentPatternProcessors.containsKey(id)
                        && currentPatternProcessors.get(id).f1 >= resultSet.getInt("version")) {
                    continue;
                }
                currentPatternProcessors.put(
                        id,
                        new Tuple4<>(
                                requireNonNull(resultSet.getString("id")),
                                resultSet.getInt("version"),
                                requireNonNull(resultSet.getString("pattern")),
                                resultSet.getString("function")));
            }
            if (latestPatternProcessors == null || isPatternProcessorUpdated(currentPatternProcessors)) {
                latestPatternProcessors = currentPatternProcessors;
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            LOG.warn("Pattern processor discoverer checks rule changes - " + e.getMessage());
        }
        return false;
    }

    private boolean isPatternProcessorUpdated(
            Map<String, Tuple4<String, Integer, String, String>> currentPatternProcessors) {
        return latestPatternProcessors.size() != currentPatternProcessors.size()
                || !currentPatternProcessors.equals(latestPatternProcessors);
    }

    @Override
    public List<PatternProcessor<T>> getLatestPatternProcessors() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new SimpleModule()
                                .addDeserializer(ConditionSpec.class, ConditionSpecStdDeserializer.INSTANCE)
                                .addDeserializer(Time.class, TimeStdDeserializer.INSTANCE)
                                .addDeserializer(NodeSpec.class, NodeSpecStdDeserializer.INSTANCE));

        return latestPatternProcessors.values().stream()
                .map(
                        patternProcessor -> {
                            try {
                                String patternStr = patternProcessor.f2;
                                GraphSpec graphSpec =
                                        objectMapper.readValue(patternStr, GraphSpec.class);
                                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
                                System.out.println(
                                        objectMapper
                                                .writerWithDefaultPrettyPrinter()
                                                .writeValueAsString(graphSpec));
                                PatternProcessFunction<T, ?> patternProcessFunction = null;
                                if (!StringUtils.isNullOrWhitespaceOnly(patternProcessor.f3)) {
                                    patternProcessFunction =
                                            (PatternProcessFunction<T, ?>)
                                                    this.userCodeClassLoader
                                                            .loadClass(patternProcessor.f3)
                                                            .newInstance();
                                }
                                LOG.warn(
                                        objectMapper
                                                .writerWithDefaultPrettyPrinter()
                                                .writeValueAsString(patternProcessor.f2));
                                return new DefaultPatternProcessor<>(
                                        patternProcessor.f0,
                                        patternProcessor.f1,
                                        patternStr,
                                        patternProcessFunction,
                                        this.userCodeClassLoader);
                            } catch (Exception e) {

                                LOG.error(
                                        "Get the latest pattern processors of the discoverer failure. - "
                                                + e.getMessage());
                                e.printStackTrace();
                            }
                            return null;
                        })
                .collect(Collectors.toList());
    }


    @Override
    public void close() throws IOException {
        super.close();
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            LOG.warn(
                    "ResultSet of the pattern processor discoverer couldn't be closed - " + e.getMessage());
        } finally {
            resultSet = null;
        }
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Statement of the pattern processor discoverer couldn't be closed - " + e.getMessage());
        } finally {
            statement = null;
        }
    }

}
