package org.apache.hadoop.hdfs.server.blockmanagement;
import java.util.*;
/**
 * @author LQL
 *
 */
public class ResidualFlowNetwork {

    private int V;
    private int eNum;
    private LinkedList<ResidualEdge>[] adj;

    public ResidualFlowNetwork(int n){
        V = n;
        eNum = 0;
        adj = new LinkedList[n];

        for(int i = 0;i<n;i++)
            adj[i] = new LinkedList<>();
    }

    public void addEdge(ResidualEdge e){
        int v1 = e.from();
        adj[v1].add(e);
        eNum++;
    }

    /**
     * @return
     */
    public LinkedList<Integer> augmentingPath(){

        LinkedList<Integer> list = new LinkedList<>();
        Queue<Integer> queue = new LinkedList<>();
        int[] reached = new int[V];
        int[] preNode = new int[V];
        for(int i = 0; i< V; i++){
            reached[i] = 0;
            preNode[i] = -1;
        }
        preNode[0] = -1;

        reached[0] = 1;
        queue.add(0);
        while(!queue.isEmpty()){
            int now = queue.poll();

            LinkedList<ResidualEdge> inlist = (LinkedList<ResidualEdge>) adj[now].clone();

            while(!inlist.isEmpty()){

                ResidualEdge e = inlist.pop();
                int v2 = e.to();

                if(reached[v2]==0){
                    queue.add(v2);
                    reached[v2] = 1;
                    preNode[v2] = now;
                }
            }
        }

//        for(int i = 0; i< V; i++){
//            System.out.println(reached[i]+"     "+preNode[i]);
//        }

        if(reached[V -1]==0){
            //System.out.println("here");
            return list;

        }

        int pointnum = V -1;
        while(pointnum!=-1){
            list.add(0, pointnum);
            pointnum = preNode[pointnum];
        }

        return list;
    }

    /**
     * @param list
     * @return
     */
    public int changeNum(LinkedList<Integer> list){
        if(list.equals(null))
            return 0;
        int minchange = Integer.MAX_VALUE;
        int v1 = 0;
        for(int i = 1;i<list.size();i++){
            int v2 = list.get(i);
            LinkedList<ResidualEdge> elist = (LinkedList<ResidualEdge>) adj[v1].clone();
            ResidualEdge edge = elist.pop();
            while(edge.to()!=v2){
                edge = elist.pop();
            }
            if(minchange>edge.getFlow())
                minchange = edge.getFlow();

            v1 = v2;
        }

        return minchange;
    }

    public void printResidualFlowNetwork(){
        for(int i = 0; i< V; i++){
            if(adj[i].size()==0){
                continue;
            }
            for(int j = 0; j< adj[i].size(); j++){
                ResidualEdge e = adj[i].get(j);
                System.out.println("[ "+e.from()+" , "+e.to()+" , "+e.getFlow()+" ]");
            }
        }
    }
}