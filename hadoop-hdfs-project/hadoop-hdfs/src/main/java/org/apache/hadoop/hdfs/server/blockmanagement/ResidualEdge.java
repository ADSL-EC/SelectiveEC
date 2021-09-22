package org.apache.hadoop.hdfs.server.blockmanagement;

/**
 * @author LQL
 *
 */
public class ResidualEdge {
    private int v;
    private int w;
    private int flow;

    public ResidualEdge(int v, int w, int flow){
        this.v = v;
        this.w = w;
        this.flow = flow;
    }

    public int from(){
        return v;
    }

    public int to(){
        return w;
    }

    public int getFlow(){
        return flow;
    }

    public void setFlow(int f){
        flow = f;
    }

}