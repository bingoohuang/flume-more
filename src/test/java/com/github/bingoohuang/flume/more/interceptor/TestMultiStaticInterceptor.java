package com.github.bingoohuang.flume.more.interceptor;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import com.github.bingoohuang.flume.more.interceptor.MultiStaticInterceptor.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestMultiStaticInterceptor {
    @Test
    public void testOneKeyValue() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
                MultiStaticInterceptor.Builder.class.getName());
        Context ctx = new Context();
        ctx.put(Constants.KEY_VALUES, "myKey:myValue");

        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        Event event = EventBuilder.withBody("test", Charsets.UTF_8);
        Assert.assertNull(event.getHeaders().get("myKey"));

        event = interceptor.intercept(event);
        String val = event.getHeaders().get("myKey");

        Assert.assertNotNull(val);
        Assert.assertEquals("myValue", val);
    }

    @Test
    public void testThreeKeyValue() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
                MultiStaticInterceptor.Builder.class.getName());
        Context ctx = new Context();
        ctx.put(Constants.KEY_VALUES, "myKey:myValue, myKey1:myValue1, myKey2:myValue2");

        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        Event event = EventBuilder.withBody("test", Charsets.UTF_8);
        Map<String, String> headers1 = event.getHeaders();
        Assert.assertNull(headers1.get("myKey"));
        Assert.assertNull(headers1.get("myKey1"));
        Assert.assertNull(headers1.get("myKey2"));

        event = interceptor.intercept(event);
        Map<String, String> headers2 = event.getHeaders();
        String val = headers2.get("myKey");
        String val1 = headers2.get("myKey1");
        String val2 = headers2.get("myKey2");

        Assert.assertNotNull(val);
        Assert.assertNotNull(val1);
        Assert.assertNotNull(val2);
        Assert.assertEquals("myValue", val);
        Assert.assertEquals("myValue1", val1);
        Assert.assertEquals("myValue2", val2);
    }

    @Test
    public void testThreeKeyValueWithCustomSeparator() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
                MultiStaticInterceptor.Builder.class.getName());
        Context ctx = new Context();
        ctx.put(Constants.KEY_VALUE_SEPARATOR, "=");
        ctx.put(Constants.ENTRY_SEPARATOR, "&");
        ctx.put(Constants.KEY_VALUES, "myKey=myValue & myKey1=myValue1 & myKey2=myValue2");

        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        Event event = EventBuilder.withBody("test", Charsets.UTF_8);
        Map<String, String> headers1 = event.getHeaders();
        Assert.assertNull(headers1.get("myKey"));
        Assert.assertNull(headers1.get("myKey1"));
        Assert.assertNull(headers1.get("myKey2"));

        event = interceptor.intercept(event);
        Map<String, String> headers2 = event.getHeaders();
        String val = headers2.get("myKey");
        String val1 = headers2.get("myKey1");
        String val2 = headers2.get("myKey2");

        Assert.assertNotNull(val);
        Assert.assertNotNull(val1);
        Assert.assertNotNull(val2);
        Assert.assertEquals("myValue", val);
        Assert.assertEquals("myValue1", val1);
        Assert.assertEquals("myValue2", val2);
    }

    @Test
    public void testReplace() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
                MultiStaticInterceptor.Builder.class.getName());
        Context ctx = new Context();
        ctx.put(Constants.PRESERVE, "false");
        ctx.put(Constants.KEY_VALUES, "myKey:replacement value");

        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        Event event = EventBuilder.withBody("test", Charsets.UTF_8);
        event.getHeaders().put("myKey", "incumbent value");

        Assert.assertNotNull(event.getHeaders().get("myKey"));

        event = interceptor.intercept(event);
        String val = event.getHeaders().get("myKey");

        Assert.assertNotNull(val);
        Assert.assertEquals("replacement value", val);
    }

    @Test
    public void testPreserve() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
                MultiStaticInterceptor.Builder.class.getName());
        Context ctx = new Context();
        ctx.put(Constants.PRESERVE, "true");
        ctx.put(Constants.KEY_VALUES, "mykey:replacement value");

        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        Event event = EventBuilder.withBody("test", Charsets.UTF_8);
        event.getHeaders().put("myKey", "incumbent value");

        Assert.assertNotNull(event.getHeaders().get("myKey"));

        event = interceptor.intercept(event);
        String val = event.getHeaders().get("myKey");

        Assert.assertNotNull(val);
        Assert.assertEquals("incumbent value", val);
    }
}