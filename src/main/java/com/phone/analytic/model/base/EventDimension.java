/**
 * Copyright (C), 2015-2018
 * FileName: EventDimension
 * Author: imyubao
 * Date: 2018/9/27 20:26
 * Description: 事件基本维度实体类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
package com.phone.analytic.model.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 功能简述: <br>
 * 事件基本维度实体类
 *
 * @author imyubao
 * @classname EventDimension
 * @create 2018/9/27
 * @since 1.0
 */
public class EventDimension extends BaseDimension {

    private int id;
    private String category;
    private String action;

    public EventDimension(int id, String category, String action) {
        this(category,action);
        this.id = id;
    }

    public EventDimension(String category, String action) {
        this.category = category;
        this.action = action;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }
        EventDimension other = (EventDimension) o;
        int temp = this.category.compareTo(other.category);
        if (temp != 0){
            return temp;
        }
        return this.action.compareTo(other.action);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.category);
        out.writeUTF(this.action);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.category = in.readUTF();
        this.action = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventDimension that = (EventDimension) o;
        return id == that.id &&
                Objects.equals(category, that.category) &&
                Objects.equals(action, that.action);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, category, action);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
