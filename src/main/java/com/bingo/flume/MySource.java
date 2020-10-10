package com.bingo.flume;

import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.Map;

/**
 * @author: jiangjiabin
 * @date: Create in 23:37 2020/10/10
 * @description:
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    //定义需要从配置中读取的字段
    /**
     * 两条数据之间的间隔
     */
    private Long delay;
    private String fields;

    @Override
    public Status process() throws EventDeliveryException {
        try {
            Map<String, String> header = Maps.newHashMap();
            SimpleEvent event = new SimpleEvent();
            //拿到数据
            for (int i = 0; i < 5; i++) {
                event.setHeaders(header);
                event.setBody((fields + i).getBytes());
                getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        delay = context.getLong("delay", 2000L);
        fields = context.getString("fields", "bingo");

    }
}
