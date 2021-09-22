/**
 * @author LQL
 *
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

public class FlowEdge {

    private int v;
    private int w;
    private int capacity;
    private int flow;

    public FlowEdge(int v, int w, int capacity, int flow){
        this.v = v;
        this.w = w;
        this.capacity = capacity;
        this.flow = flow;
    }

    public FlowEdge(FlowEdge e){
        this.v=e.from();
        this.w=e.to();
        this.capacity=e.getCapacity();
        this.flow=0;
    }

    public int from(){
        return v;
    }

    public int to(){
        return w;
    }

    public void setCapacity(int capacity){
        this.capacity=capacity;
    }

    public int getCapacity(){
        return capacity;
    }

    public int getFlow(){
        return flow;
    }

    public void setFlow(int f){
        flow = f;
    }

    public String toString(){
        return "("+v+" , "+w+" , "+capacity+" , "+flow+")";
    }
}