package com.bingo.mapreduce.groupingcomparable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author: jiangjiabin
 * @date: Create in 16:11 2020/8/29
 * @description:
 */
public class OrderComparator extends WritableComparator {

    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}
