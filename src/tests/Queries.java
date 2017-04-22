package tests;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import bufmgr.PageNotReadException;
import global.Descriptor;
import global.NID;
import global.PageId;
import global.SystemDefs;
import heap.Tuple;
import index.IndexException;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.SortMerge;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;
import queryPojo.Pair;

public class Queries {
  
    public static List<Pair<NodeQueryPojo,NodeQueryPojo>> queryPQ1a(String name,List<NodeQueryPojo> nodeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
    {
        

        List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNids= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();
        int i=0;
        List<Pair<NodeQueryPojo,NodeQueryPojo>> childQuery = new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();

        while(nodeQuery.size()>1)
        {
            if(i==0)
            {
                i=1;
                
                childQuery.add(new Pair<NodeQueryPojo,NodeQueryPojo>( nodeQuery.get(0),nodeQuery.get(0)));
            }
            Iterator<Pair<NodeQueryPojo, NodeQueryPojo>> iterQuery = childQuery.iterator();
            List<Pair<NodeQueryPojo,NodeQueryPojo>>  children = new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();

            while (iterQuery.hasNext()) {

                NestedLoopsJoins nlj;
                Pair<NodeQueryPojo,NodeQueryPojo> nextIterQuery = iterQuery.next();
                nlj  = new Join().joinNodeSEdge(name, nextIterQuery.getRight());
                Tuple t = new Tuple();
                iterator.Iterator joinResult=(iterator.Iterator)nlj;

                while ((t = joinResult.get_next()) != null) {
                    NodeQueryPojo tmp=new NodeQueryPojo();
                    tmp.setLabel(t.getStrFld(8)); // has destination Label

                    //                    NID destNid2 = new NID();
                    //                    destNid2.pageNo=new PageId(t.getIntFld(4));
                    //                    destNid2.slotNo=t.getIntFld(5);
                    //                    System.out.println("Dest Page No: "+t.getIntFld(4) +" slot: "+ t.getIntFld(5));
                    //                    Descriptor destDisc2 = new Descriptor();
                    //                    destDisc2 =  SystemDefs.JavabaseDB.nhfile.getNode(destNid2).getDesc();
                    //                    System.out.println(" Descriptor"+ destDisc2.getString());

                    NID destNid = new NID(); 
                    destNid.pageNo=new PageId(t.getIntFld(4));
                    destNid.slotNo=t.getIntFld(5);
                    
                    if(nodeQuery.get(1).getKey()==2)
                    {
                        Descriptor destDisc = new Descriptor();
                        destDisc =  SystemDefs.JavabaseDB.nhfile.getNode(destNid).getDesc();
                        //System.out.println("Destination: "+ t.getStrFld(8) +" Des: "+destDisc.getString2());
                        if(destDisc.equal(nodeQuery.get(1).getDesc())==1){
                            {
                                if(nextIterQuery.getLeft().getLabel()==null)
                                {
                                    NodeQueryPojo rtemp=new NodeQueryPojo();
                                    rtemp.setLabel(t.getStrFld(7));
                                     children.add(new Pair<NodeQueryPojo,NodeQueryPojo>( rtemp,tmp) );
                                }
                                else 
                               children.add(new Pair<NodeQueryPojo,NodeQueryPojo>( nextIterQuery.getLeft(),tmp) );

                                if(nodeQuery.size()==2)
                                {
                                    NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    if(nextIterQuery.getLeft().getLabel()==null)
                                    {
                                        NodeQueryPojo rtemp=new NodeQueryPojo();
                                        rtemp.setLabel(t.getStrFld(7));
                                         ansNids.add(new Pair<NodeQueryPojo,NodeQueryPojo>( rtemp,destNidPojo));
                                    }
                                    else 
                                    ansNids.add(new Pair<NodeQueryPojo,NodeQueryPojo>( nextIterQuery.getLeft(),destNidPojo));
                                }
                            }
                        }
                    }
                    else
                    {
                        if(t.getStrFld(8).equals(nodeQuery.get(1).getLabel())){
                            
                            if(nextIterQuery.getLeft().getLabel()==null)
                            {
                                NodeQueryPojo rtemp=new NodeQueryPojo();
                                rtemp.setLabel(t.getStrFld(7));
                                children.add(new Pair<NodeQueryPojo,NodeQueryPojo>( rtemp,tmp) );
                            }
                            else 
                                children.add(new Pair<NodeQueryPojo,NodeQueryPojo>( nextIterQuery.getLeft(),tmp) );
                            
                            if(nodeQuery.size()==2)
                            {
                                NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                destNidPojo.setNd(destNid);
                                if(nextIterQuery.getLeft().getLabel()==null)
                                {
                                    NodeQueryPojo rtemp=new NodeQueryPojo();
                                    rtemp.setLabel(t.getStrFld(7));
                                    ansNids.add(new Pair<NodeQueryPojo,NodeQueryPojo>( rtemp,destNidPojo));
                                }
                                else 
                                ansNids.add(new Pair<NodeQueryPojo,NodeQueryPojo>( nextIterQuery.getLeft(),destNidPojo) );
                            }
                        }
                    }  

                }
                nlj.close();
            }

            nodeQuery.remove(0);
            childQuery=children;
        }
        return ansNids;
    }
    
    
    
    
    public static List<NodeQueryPojo> queryPE1(String name,List<NodeQueryPojo> nodeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
    {

        List<NodeQueryPojo> ansNids= new ArrayList<NodeQueryPojo>();
        int i=0;
        List<NodeQueryPojo> childQuery = new ArrayList<NodeQueryPojo>();

        while(nodeQuery.size()>1)
        {
            if(i==0)
            {
                i=1;
                childQuery.add(nodeQuery.get(0));
            }
            Iterator<NodeQueryPojo> iterQuery = childQuery.iterator();
            List<NodeQueryPojo> children = new ArrayList<NodeQueryPojo>();

            while (iterQuery.hasNext()) {

                NestedLoopsJoins nlj;

                nlj  = new Join().joinNodeSEdge(name, iterQuery.next());
                Tuple t = new Tuple();
                iterator.Iterator joinResult=(iterator.Iterator)nlj;

                while ((t = joinResult.get_next()) != null) {
                    NodeQueryPojo tmp=new NodeQueryPojo();
                    tmp.setLabel(t.getStrFld(8)); // has destination Label
                    NID destNid = new NID(); 
                    destNid.pageNo=new PageId(t.getIntFld(4));
                    destNid.slotNo=t.getIntFld(5);

                    if(nodeQuery.get(1).getKey()==2)
                    {
                        Descriptor destDisc = new Descriptor();
                        destDisc =  SystemDefs.JavabaseDB.nhfile.getNode(destNid).getDesc();
                        //System.out.println("Destination: "+ t.getStrFld(8) +" Des: "+destDisc.getString2());
                        if(destDisc.equal(nodeQuery.get(1).getDesc())==1){
                            {
                                children.add(tmp);

                                if(nodeQuery.size()==2)
                                {
                                    NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    ansNids.add(destNidPojo);
                                    
                                }
                            }
                        }
                    }
                    else
                    {
                        if(t.getStrFld(8).equals(nodeQuery.get(1).getLabel())){
                            children.add(tmp);
                            
                            if(nodeQuery.size()==2)
                            {
                                NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                destNidPojo.setNd(destNid);
                                ansNids.add(destNidPojo);
                            }
                        }
                    }  

                }
                nlj.close();
            }

            nodeQuery.remove(0);
            childQuery=children;
        }
        return ansNids;
    }
    
    

 public static List<EdgeQueryPojo> queryPE2(String name,List<EdgeQueryPojo> edgeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
    {
        List<EdgeQueryPojo> ansNids= new LinkedList<EdgeQueryPojo>();
        List<EdgeQueryPojo> childQuery = new LinkedList<EdgeQueryPojo>();
        List<NodeQueryPojo> nchildQuery = new LinkedList<NodeQueryPojo>();
        int i=0;
        
        while(edgeQuery.size()>1)
        {
              List<EdgeQueryPojo> children = new LinkedList<EdgeQueryPojo>();
            if(i==0)
            {
                
                NID nid=edgeQuery.get(0).getNd();
                String node_label=SystemDefs.JavabaseDB.nhfile.getNode(nid).getLabel();
                NodeQueryPojo npj=new NodeQueryPojo();
                npj.setLabel(node_label);
                nchildQuery.add(npj);
                Iterator<NodeQueryPojo> iterQuery = nchildQuery.iterator();
                while (iterQuery.hasNext()) {
                    NestedLoopsJoins nlj;

                    nlj  = new Join().joinNodeSEdge(name, iterQuery.next());
                    Tuple t = new Tuple();
                    iterator.Iterator joinResult=(iterator.Iterator)nlj;
                    while ((t = joinResult.get_next()) != null) {
                        EdgeQueryPojo tmp=new EdgeQueryPojo();
                        tmp.setEdgelabel(t.getStrFld(6)); 
                        NID destNid = new NID(); 
                        destNid.pageNo=new PageId(t.getIntFld(4));
                        destNid.slotNo=t.getIntFld(5);
                        String label=t.getStrFld(6);
                        int wt=t.getIntFld(1);
                        if(edgeQuery.get(1).getKey()==1)
                        {
                            if(edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label))
                            {
                                children.add(tmp);
                                if(edgeQuery.size()==2)
                                {
                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    ansNids.add(destNidPojo);
                                }
                            }
                            
                        }
                       else if(edgeQuery.get(1).getKey()==2)
                       {
                           if(edgeQuery.get(1).getWeight()>=wt)
                           {
                               children.add(tmp);
                               if(edgeQuery.size()==2)
                               {
                                   EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                   destNidPojo.setNd(destNid);
                                   ansNids.add(destNidPojo);
                               }
                           }
                       }
                        
                    }
                    
                    
                }
               
            }
            else{
                
                EdgeQueryPojo epj=new EdgeQueryPojo();
                Iterator<EdgeQueryPojo> iterQuery=childQuery.iterator();
                while (iterQuery.hasNext()) {
                    SortMerge sm=new  Join().joinEdgeEdge(name, iterQuery.next());
                     Tuple t = new Tuple();
                     while ((t = sm.get_next()) != null) {
                         EdgeQueryPojo tmp=new EdgeQueryPojo(); 
                         String label=t.getStrFld(6);
                         tmp.setEdgelabel(label);
                         NID destNid = new NID(); 
                        //System.out.println("label is "+t.getStrFld(6));
                          destNid.pageNo=new PageId(t.getIntFld(4));
                          destNid.slotNo=t.getIntFld(5);
                          
                          int wt=t.getIntFld(1);
                          if(edgeQuery.get(1).getKey()==1)
                          {
                            if(edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label))
                            {
                                children.add(tmp);
                                if(edgeQuery.size()==2)
                                  {
                                      EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                      destNidPojo.setNd(destNid);
                                      ansNids.add(destNidPojo);
                                  }
                            }
                            
                            
                          }
                          else if(edgeQuery.get(1).getKey()==2)
                          {
                           if(edgeQuery.get(1).getWeight()>=wt)
                           {
                               children.add(tmp);
                               if(edgeQuery.size()==2)
                                  {
                                      EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                      destNidPojo.setNd(destNid);
                                      ansNids.add(destNidPojo);
                                  }
                           }
                          }
                         
                     }
                     try {
                        sm.close();
                      Join.f.deleteFile();
                       Join.fN.deleteFile();
                    } catch (Exception e) {
                        
                        e.printStackTrace();
                    }
                }
                
                
            }
            //System.out.println("children in iteration"+i);
            
        
            childQuery=children;
            /*for(EdgeQueryPojo epj:childQuery)
            {
                System.out.println(epj.getEdgelabel());
            }*/
            edgeQuery.remove(0);
            i++;
        }
        
       
        return ansNids;
    }
    
    
    
}


