package com.github.bingoohuang.flume.more.interceptor;

import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.interceptor.HostInterceptor;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interceptor class that appends multiple static, pre-configured headers to all events.
 * <p/>
 * Properties:<p>
 * <p/>
 * keyValues: Key:value pairs to use in static header insertion.<p>
 * <p/>
 * preserveExisting: Whether to preserve an existing value for 'key'
 * (default is true)<p>
 * <p/>
 * Sample config:<p>
 * <p/>
 * <code>
 * agent.sources.r1.channels = c1<p>
 * agent.sources.r1.type = SEQ<p>
 * agent.sources.r1.interceptors = i1<p>
 * agent.sources.r1.interceptors.i1.type = com.github.bingoohuang.flume.more.interceptor.MultiStaticInterceptor<p>
 * agent.sources.r1.interceptors.i1.preserveExisting = false<p>
 * agent.sources.r1.interceptors.i1.keyValues = key1:value1,key2:value2<p>
 * </code>
 */
public class MultiStaticInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(MultiStaticInterceptor.class);

    private final boolean preserveExisting;
    private final Map<String, String> keyValues;

    /**
     * Only {@link HostInterceptor.Builder} can build me
     */
    private MultiStaticInterceptor(boolean preserveExisting, Map<String, String> keyValues) {
        this.preserveExisting = preserveExisting;
        this.keyValues = keyValues;
    }

    @Override
    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            if (preserveExisting && headers.containsKey(key)) continue;

            headers.put(key, entry.getValue());
        }

        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instance of the MultiStaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {
        private boolean preserveExisting;
        private String keyValues;
        private String keyValueSeparator;
        private String entrySeparator;

        @Override
        public void configure(Context context) {
            preserveExisting = context.getBoolean(Constants.PRESERVE, Constants.PRESERVE_DEFAULT);
            keyValues = context.getString(Constants.KEY_VALUES);
            if (StringUtils.isEmpty(keyValues))
                throw new ConfigurationException("KeyValues is null !");

            keyValueSeparator = context.getString(Constants.KEY_VALUE_SEPARATOR, Constants.KEY_VALUE_SEPARATOR_DEFAULT);
            entrySeparator = context.getString(Constants.ENTRY_SEPARATOR, Constants.ENTRY_SEPARATOR_DEFAULT);
        }

        @Override
        public Interceptor build() {
            logger.info(String.format("Creating StaticInterceptor: preserveExisting=%s,keyValues=%s",
                    preserveExisting, keyValues));

            Map<String, String> kvMap = new HashMap<String, String>();
            Splitter entrySeparator = Splitter.on(this.entrySeparator).omitEmptyStrings().trimResults();
            Iterable<String> kvPairs = entrySeparator.split(keyValues);
            for (String pair : kvPairs) {
                Map.Entry<String, String> entry = parseEntry(pair);
                if (entry != null) kvMap.put(entry.getKey(), entry.getValue());
            }

            return new MultiStaticInterceptor(preserveExisting, kvMap);
        }

        private Map.Entry<String, String> parseEntry(final String pair) {
            final int pos = pair.indexOf(keyValueSeparator);
            if (pos < 0)
                throw new ConfigurationException("KeyValues format error : " + pair);

            String key = StringUtils.trim(pair.substring(0, pos));
            if (StringUtils.isEmpty(key))
                throw new ConfigurationException("The key of keyValues is empty : " + pair);

            return new Map.Entry<String, String>() {

                @Override
                public String getKey() {
                    return StringUtils.trim(pair.substring(0, pos));
                }

                @Override
                public String getValue() {
                    int length = keyValueSeparator.length();
                    return StringUtils.trim(StringUtils.substring(pair, pos + length));
                }

                @Override
                public String setValue(String value) {
                    return null;
                }
            };
        }

    }

    public interface Constants {
        String KEY_VALUES = "keyValues";

        String KEY_VALUE_SEPARATOR = "keyValueSeparator";
        String KEY_VALUE_SEPARATOR_DEFAULT = ":";
        String ENTRY_SEPARATOR = "entrySeparator,";
        String ENTRY_SEPARATOR_DEFAULT = ",";

        String PRESERVE = "preserveExisting";
        boolean PRESERVE_DEFAULT = true;
    }
}