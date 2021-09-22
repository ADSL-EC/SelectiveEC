package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.*;
/**
 * @author LQL
 *
 */
public class FlowNetwork {

    private int V;
    private int eNum;
    private ResidualFlowNetwork residualFlowNetwork;
    private LinkedList<FlowEdge>[] adj;
    private ArrayList<LinkedList<Integer>> paths;

    public FlowNetwork(int n){
        V = n;
        eNum = 0;
        adj = new LinkedList[n];
        paths=new ArrayList<>();

        for(int i = 0;i<n;i++)
            adj[i] = new LinkedList<>();
    }

    public FlowNetwork(FlowNetwork fn){
        this.V=fn.V();
        this.eNum=0;
        this.adj=new LinkedList[fn.V()];
        this.paths=new ArrayList<>();
        for (int i = 0; i < adj.length; i++) {
            this.adj[i] = new LinkedList();
        }

        for(int i=0;i<fn.V();i++){
            for(FlowEdge e:fn.adj(i)){
                this.adj[i].add(new FlowEdge(e));
            }
        }
    }

    public Iterable<FlowEdge> adj(int v) {
        return adj[v];
    }

    public int V() {
        return V;
    }

    public void addEdge(FlowEdge e){
        int v1 = e.from();
        adj[v1].add(e);
        eNum++;
    }

    public void resetFlow(){
        for(int i=0;i<V;i++){
            for(FlowEdge e:adj[i]){
                e.setFlow(0);
            }
        }
    }

    public void deleteNodeEdge(int nodeId){
        eNum-=adj[nodeId].size();
        adj[nodeId]=new LinkedList();
    }

    public void deleteNodeFullEdge(int nodeId){
        List<Integer> removeIndexes=new ArrayList<>();
//        System.out.println("adj[nodeId].size()"+adj[nodeId].size());
        for(int i=0;i<adj[nodeId].size();i++){
            if(adj[nodeId].get(i).getFlow()!=0){
                removeIndexes.add(i);
//                System.out.println(i);
                break;
            }
        }
        for(int i:removeIndexes){
            adj[nodeId].remove(i);
            eNum-=1;
        }
    }

    @Override
    public String toString() {
        String result = "";
        for (int i = 0; i < V; i++) {
            result += i + ":";
            for(FlowEdge e:adj[i]) {
                result += " " + e.toString();
            }
            result += "\n";
        }
        return result;
    }

//	public void deleteEdge(FlowEdge e){
//		int v1 = e.from();
//		int v2 = e.from();
//
//	}

    public void produceGf(){
        residualFlowNetwork = new ResidualFlowNetwork(V);

        for(int i = 0; i< V; i++){
            LinkedList<FlowEdge> list = (LinkedList<FlowEdge>) adj[i].clone();

            while(!list.isEmpty()){

                FlowEdge flowEdge = list.pop();
                int v1 = flowEdge.from();
                int v2 = flowEdge.to();
                int flow = flowEdge.getFlow();
                int capacity = flowEdge.getCapacity();

                if(flow==0){
                    residualFlowNetwork.addEdge(new ResidualEdge(v1,v2,capacity));
                }else{
                    if(flow==capacity){
                        residualFlowNetwork.addEdge(new ResidualEdge(v2,v1,capacity));
                    }else if(flow<capacity){
                        residualFlowNetwork.addEdge(new ResidualEdge(v1,v2,capacity-flow));
                        residualFlowNetwork.addEdge(new ResidualEdge(v2,v1,flow));
                    }
                }
            }
        }
    }

    public ResidualFlowNetwork getResidualFlowNetwork(){
        return residualFlowNetwork;
    }

    private LinkedList<Integer> augmentingPath(){
        return residualFlowNetwork.augmentingPath();
    }

    private int changeNum(LinkedList<Integer> list){
        return residualFlowNetwork.changeNum(list);
    }

    public void resetEdgeFlow(int from, int to, int value){
        for(FlowEdge e:adj[from]){
            if(e.to()==to){
                e.setFlow(value);
                break;
            }
        }
    }

    public void modifyEdgeFlow(int from, int to, int changeValue){
        for(FlowEdge e:adj[from]){
            if(e.to()==to){
                e.setFlow(e.getFlow()+changeValue);
                break;
            }
        }
    }

    public void adjustFlowPath(int nodeId){
        ArrayList<LinkedList<Integer>> removeList=new ArrayList<>();

        for(LinkedList<Integer> path:paths){
            boolean flag=false;

            for(int pid:path){
                if(pid==nodeId){
//                    System.out.println(path);
                    removeList.add(path);
                    flag=true;
                    break;
                }
            }

            if(flag==true){
                LinkedList<Integer> copylist = (LinkedList<Integer>) path.clone();

                copylist.pop();
                int from=copylist.pop();
                int to=copylist.pop();
                resetEdgeFlow(0,from,0);
                resetEdgeFlow(from,to,0);

                while(copylist.size()!=1){
                    from=copylist.pop();
                    resetEdgeFlow(from,to,1);
                    to=copylist.pop();
                    resetEdgeFlow(from,to,0);
                }

                modifyEdgeFlow(to,V-1,-1);
            }
        }

        for(LinkedList<Integer> path:removeList){
            paths.remove(path);
        }
    }

    public int getMaxFlow(){
        setPaths(new ArrayList<>());
        produceGf();
//        residualFlowNetwork.printResidualFlowNetwork();
        LinkedList<Integer> list = augmentingPath();

        while(list.size()>0){

            getPaths().add((LinkedList<Integer>) list.clone());

            int changenum = changeNum(list);

//            LinkedList<Integer> copylist = (LinkedList<Integer>) list.clone();//调试
//            System.out.println("list:");
//            while(!copylist.isEmpty()){
//                System.out.print(copylist.pop()+"  ");
//            }
//            System.out.println();
//            System.out.println("changenum: "+changenum);

            int v1 = 0;
            for(int i = 1;i<list.size();i++){
                int v2 = list.get(i);
                boolean flag=true;
                if(!adj[v1].isEmpty()){
                    int j = 0;
                    FlowEdge e = adj[v1].get(j);
                    while(e.to()!=v2 && j< adj[v1].size()){
                        e = adj[v1].get(j);
                        j++;
                    }
                    if(e.to()!=v2 && j== adj[v1].size()){//调试
                        flag=false;
                        j = 0;
                        e = adj[v2].get(j);
                        while(e.to()!=v1 && j< adj[v2].size()){
                            e = adj[v2].get(j);
                            j++;
                        }

                    }
                    if(flag){
                        e.setFlow(e.getFlow()+changenum);
                    }
                    else{
                        e.setFlow(e.getFlow()-changenum);
                    }
                }
                v1  = v2;

            }
//            printFlowNetwork();
            produceGf();
//            residualFlowNetwork.printResidualFlowNetwork();
            list = augmentingPath();
        }

//        printFlowNetwork();

        return getValue();
    }

    public int getValue(){
        int maxflow = 0;

        for(int i = 0; i< V; i++){
            if(adj[i].size()>0){
                for(int j = 0; j< adj[i].size(); j++){
                    if(adj[i].get(j).to() == V -1){
                        maxflow += adj[i].get(j).getFlow();
                    }
                }
            }
        }

        return maxflow;
    }

    public void printFlowNetwork(){
        for(int i = 0; i< V; i++){
            if(adj[i].size()==0)
                continue;
            for(int j = 0; j< adj[i].size(); j++){
                FlowEdge e = adj[i].get(j);
                System.out.println("[ "+e.from()+" , "+e.to()+" , "+e.getFlow()+" , "+e.getCapacity()+" ]");
            }
        }
    }

    public void printPaths(){
        String result="<------------------------>\n";
        for(int i = 0; i< getPaths().size(); i++){
            result+="Path "+i+": ";
            LinkedList<Integer> copylist = (LinkedList<Integer>) getPaths().get(i).clone();

            while(!copylist.isEmpty()){
                result+=copylist.pop()+"  ";
            }
            result+="\n";
        }
        result+="<------------------------>\n";
        System.out.println(result);
    }

    public ArrayList<LinkedList<Integer>> getPaths() {
        return paths;
    }

    public void setPaths(ArrayList<LinkedList<Integer>> paths) {
        this.paths = paths;
    }

//    public void showResult(){
//        printFlowNetwork();
//        int maxflow = 0;
//
//        for(int i = 0; i< V; i++){
//            if(adj[i].size()>0){
//                for(int j = 0; j< adj[i].size(); j++){
//                    if(adj[i].get(j).to() == V -1){
//                        maxflow += adj[i].get(j).getFlow();
//                    }
//                }
//            }
//        }
//
//    }

}