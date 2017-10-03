package com.amazon.kinesis.streaming.agent.processing.parsers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.kinesis.streaming.agent.config.ConfigurationException;
import com.amazon.kinesis.streaming.agent.processing.exceptions.LogParsingException;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory.LogFormat;
import com.google.common.collect.ImmutableList;

/**
 * Class for parsing and transforming records of mongodb log files
 *
 *
 * timestamp, severity, component, context, message
 *
 * @author Cihan Cetin
 *
 */
public class MongodbLogParser extends BaseLogParser {

    public static final List<String> SYSLOG_FIELDS =
            ImmutableList.of("timestamp",
                             "severity",
                             "component",
                             "context",
                             "message");

    public static final Pattern RFC3339_MONGODB_PATTERN = Pattern.compile(PatternConstants.RFC3339_MONGODB_BASE);

    public MongodbLogParser(LogFormat format, String matchPattern, List<String> customFields) {
        super(format, matchPattern, customFields);
    }

    @Override
    public Map<String, Object> parseLogRecord(String record, List<String> fields) throws LogParsingException {
        if (fields == null) {
            fields = getFields();
        }
        final Map<String, Object> recordMap = new LinkedHashMap<String, Object>();
        Matcher matcher = logEntryPattern.matcher(record);

        if (!matcher.matches()) {
            throw new LogParsingException("Invalid log entry given the entry pattern");
        }

        if (matcher.groupCount() != fields.size()) {
            throw new LogParsingException("The parsed fields don't match the given fields");
        }

        for (int i = 0; i < fields.size(); i++) {
            String value = matcher.group(i + 1);
            value = value != null ? value.trim() : value;
            recordMap.put(fields.get(i), value);
        }

        return recordMap;
    }

    @Override
    protected void initializeByDefaultFormat(LogFormat format) {
        switch (format) {
            case RFC3339MONGODB:
                this.logEntryPattern = RFC3339_MONGODB_PATTERN;
                this.fields = SYSLOG_FIELDS;
                return;
            default:
                throw new ConfigurationException("Log format is not accepted");
    }
    }

}
