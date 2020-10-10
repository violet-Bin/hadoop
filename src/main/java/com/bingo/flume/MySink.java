package com.bingo.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author: jiangjiabin
 * @date: Create in 0:00 2020/10/11
 * @description:
 */
public class MySink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(MySink.class);

    private String prefix = "";
    private String suffix = "";

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event take;
            while ((take = channel.take()) == null) {
                Thread.sleep(200);
            }
            //到这里就拿到数据了
            log.info(prefix + new String(take.getBody()) + suffix);
            transaction.commit();
            status = Status.READY;
        } catch (Throwable e) {
            transaction.rollback();
            status = Status.BACKOFF;
            if (e instanceof Error) {
                throw (Error) e;
            }
        } finally {
            transaction.close();
        }

        return status;
    }

    @Override
    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("prefix", "hello:");
        //读取配置文件内容，无默认值
        suffix = context.getString("suffix");

    }
}
