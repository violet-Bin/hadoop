package com.bingo.hadoop.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 20:29 2020/8/29
 * @description:
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String id;
    private String pid;
    private int amount;
    private String pname;

    @Override
    public int compareTo(OrderBean o) {
        int compare = this.pid.compareTo(o.pid);
        if (compare == 0) {
            return o.pname.compareTo(this.pname);
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id='" + id + '\'' +
                ", pid='" + pid + '\'' +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                '}';
    }
}
