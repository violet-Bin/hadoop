package com.bingo.mapreduce.groupingcomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 15:57 2020/8/29
 * @description:
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private String productId;
    private double price;


    @Override
    public int compareTo(OrderBean o) {
        int compare = this.orderId.compareTo(o.orderId);

        if (compare == 0) {
            return Double.compare(o.price, this.price);
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(productId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        productId = dataInput.readUTF();
        price = dataInput.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", price=" + price +
                '}';
    }
}
