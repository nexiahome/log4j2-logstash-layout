package com.vlkan.log4j2.logstash.layout;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vlkan.log4j2.logstash.layout.resolver.*;
import com.vlkan.log4j2.logstash.layout.util.ByteBufferOutputStream;
import com.vlkan.log4j2.logstash.layout.util.JsonGenerators;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.ByteBufferDestination;
import org.apache.logging.log4j.core.layout.ByteBufferDestinationHelper;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
import org.apache.logging.log4j.core.util.NetUtils;
import org.apache.logging.log4j.util.Supplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;

@Plugin(name = "LogstashLayout",
        category = Node.CATEGORY,
        elementType = Layout.ELEMENT_TYPE,
        printObject = true)
public class LogstashLayout extends AbstractStringLayout {

    private final String contentType = "application/json; charset=" + getCharset();

    private final byte[] emptyObjectJsonBytes = "{}".getBytes(getCharset());

    private final TemplateResolver<LogEvent> eventResolver;

    private final byte[] lineSeparatorBytes;

    private final Supplier<LogstashLayoutSerializationContext> serializationContextSupplier;

    protected LogstashLayout(Builder builder) {

        super(builder.getCharset());

        // Create StackTraceElement resolver.
        ObjectMapper objectMapper = new ObjectMapper();
        StrSubstitutor substitutor = builder.config.getStrSubstitutor();
        TemplateResolver<StackTraceElement> stackTraceElementObjectResolver = null;
        if (builder.stackTraceEnabled) {
            StackTraceElementObjectResolverContext stackTraceElementObjectResolverContext =
                    StackTraceElementObjectResolverContext
                    .newBuilder()
                    .setObjectMapper(objectMapper)
                    .setSubstitutor(substitutor)
                    .setEmptyPropertyExclusionEnabled(builder.emptyPropertyExclusionEnabled)
                    .build();
            String stackTraceElementTemplate = readStackTraceElementTemplate(builder);
            stackTraceElementObjectResolver = TemplateResolvers.ofTemplate(stackTraceElementObjectResolverContext, stackTraceElementTemplate);
        }

        // Create LogEvent resolver.
        String eventTemplate = readEventTemplate(builder);
        FastDateFormat timestampFormat = readDateFormat(builder);
        EventResolverContext resolverContext = EventResolverContext
                .newBuilder()
                .setObjectMapper(objectMapper)
                .setSubstitutor(substitutor)
                .setTimestampFormat(timestampFormat)
                .setLocationInfoEnabled(builder.locationInfoEnabled)
                .setStackTraceEnabled(builder.stackTraceEnabled)
                .setStackTraceElementObjectResolver(stackTraceElementObjectResolver)
                .setEmptyPropertyExclusionEnabled(builder.emptyPropertyExclusionEnabled)
                .setMdcKeyPattern(builder.mdcKeyPattern)
                .setNdcPattern(builder.ndcPattern)
                .build();
        this.eventResolver = TemplateResolvers.ofTemplate(resolverContext, eventTemplate);

        // Create the serialization context supplier.
        this.lineSeparatorBytes = builder.lineSeparator.getBytes(getCharset());
        this.serializationContextSupplier = LogstashLayoutSerializationContexts.createSupplier(
                objectMapper,
                builder.maxByteCount,
                builder.prettyPrintEnabled,
                builder.emptyPropertyExclusionEnabled);

    }

    private static String readEventTemplate(Builder builder) {
        return readTemplate(builder.eventTemplate, builder.eventTemplateUri);
    }

    private static String readStackTraceElementTemplate(Builder builder) {
        return readTemplate(builder.stackTraceElementTemplate, builder.stackTraceElementTemplateUri);
    }

    private static String readTemplate(String template, String templateUri) {
        if (!StringUtils.isBlank(template)) {
            return template;
        }
        try {
            ConfigurationSource configSource = ConfigurationSource.fromUri(NetUtils.toURI(templateUri));
            return new String(configSource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception error) {
            String message = String.format("failed reading URI (spec=%s)", templateUri);
            throw new RuntimeException(message, error);
        }
    }

    private static FastDateFormat readDateFormat(Builder builder) {
        TimeZone timeZone = TimeZone.getTimeZone(builder.timeZoneId);
        return FastDateFormat.getInstance(builder.dateTimeFormatPattern, timeZone);
    }

    @Override
    public String toSerializable(LogEvent event) {
        try (LogstashLayoutSerializationContext context = serializationContextSupplier.get()) {
            encode(event, context);
            return context.getOutputStream().toString(getCharset());
        } catch (Exception error) {
            throw new RuntimeException("failed serializing JSON", error);
        }
    }

    @Override
    public void encode(LogEvent event, ByteBufferDestination destination) {
        try (LogstashLayoutSerializationContext context = serializationContextSupplier.get()) {
            encode(event, context);
            ByteBuffer byteBuffer = context.getOutputStream().getByteBuffer();
            byteBuffer.flip();
            ByteBufferDestinationHelper.writeToUnsynchronized(byteBuffer, destination);
        } catch (Exception error) {
            throw new RuntimeException("failed serializing JSON", error);
        }
    }

    private void encode(LogEvent event, LogstashLayoutSerializationContext context) throws IOException {
        try {
            unsafeEncode(event, context);
        } catch (JsonGenerationException ignored) {
            JsonGenerators.rescueJsonGeneratorState(context.getOutputStream().getByteBuffer(), context.getJsonGenerator());
            unsafeEncode(event, context);
        }
    }

    private void unsafeEncode(LogEvent event, LogstashLayoutSerializationContext context) throws IOException {
        JsonGenerator jsonGenerator = context.getJsonGenerator();
        eventResolver.resolve(event, jsonGenerator);
        jsonGenerator.flush();
        ByteBufferOutputStream outputStream = context.getOutputStream();
        if (outputStream.getByteBuffer().position() == 0) {
            outputStream.write(emptyObjectJsonBytes);
        }
        outputStream.write(lineSeparatorBytes);
    }

    @Override
    public byte[] getFooter() {
        return null;
    }

    @Override
    public byte[] getHeader() {
        return null;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public Map<String, String> getContentFormat() {
        return Collections.emptyMap();
    }

    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }

    public static class Builder<B extends Builder<B>> extends AbstractStringLayout.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<LogstashLayout> {

        public Builder() {
            super();
            setCharset(StandardCharsets.UTF_8);
        }

        @PluginConfiguration
        private Configuration config;

        @PluginBuilderAttribute
        private boolean prettyPrintEnabled = false;

        @PluginBuilderAttribute
        private boolean locationInfoEnabled = false;

        @PluginBuilderAttribute
        private boolean stackTraceEnabled = false;

        @PluginBuilderAttribute
        private boolean emptyPropertyExclusionEnabled = true;

        @PluginBuilderAttribute
        private String dateTimeFormatPattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZ";

        @PluginBuilderAttribute
        private String timeZoneId = TimeZone.getDefault().getID();

        @PluginBuilderAttribute
        private String eventTemplate = null;

        @PluginBuilderAttribute
        private String eventTemplateUri = "classpath:LogstashJsonEventLayoutV1.json";

        @PluginBuilderAttribute
        private String stackTraceElementTemplate = null;

        @PluginBuilderAttribute
        private String stackTraceElementTemplateUri = "classpath:Log4j2StackTraceElementLayout.json";

        @PluginBuilderAttribute
        private String mdcKeyPattern;

        @PluginBuilderAttribute
        private String ndcPattern;

        @PluginBuilderAttribute
        private String lineSeparator = System.lineSeparator();

        @PluginBuilderAttribute
        private int maxByteCount = 1024 * 512;  // 512 KiB

        public Configuration getConfiguration() {
            return config;
        }

        public B setConfiguration(Configuration configuration) {
            this.config = configuration;
            return this.asBuilder();
        }

        public boolean isPrettyPrintEnabled() {
            return prettyPrintEnabled;
        }

        public B setPrettyPrintEnabled(boolean prettyPrintEnabled) {
            this.prettyPrintEnabled = prettyPrintEnabled;
            return this.asBuilder();
        }

        public boolean isLocationInfoEnabled() {
            return locationInfoEnabled;
        }

        public B setLocationInfoEnabled(boolean locationInfoEnabled) {
            this.locationInfoEnabled = locationInfoEnabled;
            return this.asBuilder();
        }

        public boolean isStackTraceEnabled() {
            return stackTraceEnabled;
        }

        public B setStackTraceEnabled(boolean stackTraceEnabled) {
            this.stackTraceEnabled = stackTraceEnabled;
            return this.asBuilder();
        }

        public boolean isEmptyPropertyExclusionEnabled() {
            return emptyPropertyExclusionEnabled;
        }

        public B setEmptyPropertyExclusionEnabled(boolean emptyPropertyExclusionEnabled) {
            this.emptyPropertyExclusionEnabled = emptyPropertyExclusionEnabled;
            return this.asBuilder();
        }

        public String getDateTimeFormatPattern() {
            return dateTimeFormatPattern;
        }

        public B setDateTimeFormatPattern(String dateTimeFormatPattern) {
            this.dateTimeFormatPattern = dateTimeFormatPattern;
            return this.asBuilder();
        }

        public String getTimeZoneId() {
            return timeZoneId;
        }

        public B setTimeZoneId(String timeZoneId) {
            this.timeZoneId = timeZoneId;
            return this.asBuilder();
        }

        public String getEventTemplate() {
            return eventTemplate;
        }

        public B setEventTemplate(String eventTemplate) {
            this.eventTemplate = eventTemplate;
            return this.asBuilder();
        }

        public String getEventTemplateUri() {
            return eventTemplateUri;
        }

        public B setEventTemplateUri(String eventTemplateUri) {
            this.eventTemplateUri = eventTemplateUri;
            return this.asBuilder();
        }

        public String getStackTraceElementTemplate() {
            return stackTraceElementTemplate;
        }

        public B setStackTraceElementTemplate(String stackTraceElementTemplate) {
            this.stackTraceElementTemplate = stackTraceElementTemplate;
            return this.asBuilder();
        }

        public String getStackTraceElementTemplateUri() {
            return stackTraceElementTemplateUri;
        }

        public B setStackTraceElementTemplateUri(String stackTraceElementTemplateUri) {
            this.stackTraceElementTemplateUri = stackTraceElementTemplateUri;
            return this.asBuilder();
        }

        public String getMdcKeyPattern() {
            return mdcKeyPattern;
        }

        public B setMdcKeyPattern(String mdcKeyPattern) {
            this.mdcKeyPattern = mdcKeyPattern;
            return this.asBuilder();
        }

        public String getNdcPattern() {
            return ndcPattern;
        }

        public B setNdcPattern(String ndcPattern) {
            this.ndcPattern = ndcPattern;
            return this.asBuilder();
        }

        public String getLineSeparator() {
            return lineSeparator;
        }

        public B setLineSeparator(String lineSeparator) {
            this.lineSeparator = lineSeparator;
            return this.asBuilder();
        }

        public int getMaxByteCount() {
            return maxByteCount;
        }

        public B setMaxByteCount(int maxByteCount) {
            this.maxByteCount = maxByteCount;
            return this.asBuilder();
        }

        @Override
        public LogstashLayout build() {
            validate();
            return new LogstashLayout(this);
        }

        private void validate() {
            Validate.notNull(config, "config");
            Validate.notBlank(dateTimeFormatPattern, "dateTimeFormatPattern");
            Validate.notBlank(timeZoneId, "timeZoneId");
            Validate.isTrue(
                    !StringUtils.isBlank(eventTemplate) || !StringUtils.isBlank(eventTemplateUri),
                    "both eventTemplate and eventTemplateUri are blank");
            if (stackTraceEnabled) {
                Validate.isTrue(
                        !StringUtils.isBlank(stackTraceElementTemplate) || !StringUtils.isBlank(stackTraceElementTemplateUri),
                        "both stackTraceElementTemplate and stackTraceElementTemplateUri are blank");
            }
            Validate.isTrue(maxByteCount > 0, "maxByteCount requires a non-zero positive integer");
        }

        @Override
        public String toString() {
            String escapedLineSeparator = lineSeparator.replace("\\", "\\\\");
            return "Builder{prettyPrintEnabled=" + prettyPrintEnabled +
                    ", locationInfoEnabled=" + locationInfoEnabled +
                    ", stackTraceEnabled=" + stackTraceEnabled +
                    ", emptyPropertyExclusionEnabled=" + emptyPropertyExclusionEnabled +
                    ", dateTimeFormatPattern='" + dateTimeFormatPattern + '\'' +
                    ", timeZoneId='" + timeZoneId + '\'' +
                    ", eventTemplate='" + eventTemplate + '\'' +
                    ", eventTemplateUri='" + eventTemplateUri + '\'' +
                    ", mdcKeyPattern='" + mdcKeyPattern + '\'' +
                    ", lineSeparator='" + escapedLineSeparator + '\'' +
                    ", maxByteCount='" + maxByteCount + '\'' +
                    '}';
        }

    }

}
