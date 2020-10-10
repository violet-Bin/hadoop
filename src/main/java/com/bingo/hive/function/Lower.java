package com.bingo.hive.function;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author: jiangjiabin
 * @date: Create in 21:18 2020/10/6
 * @description:
 */
public class Lower extends UDF {

    public String evaluate (final String s) {

        if (s == null) {
            return null;
        }
        return s.toLowerCase();
    }

}
