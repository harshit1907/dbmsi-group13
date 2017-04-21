package tests;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import javax.sound.midi.Soundbank;

//import com.sun.org.apache.regexp.internal.RE;

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

public class Queries {




    public static List<NodeQueryPojo> queryPE1(String name,List<NodeQueryPojo> nodeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
    {

        List<NodeQueryPojo> ansNids= new LinkedList<NodeQueryPojo>();
        int i=0;
        List<NodeQueryPojo> childQuery = new LinkedList<NodeQueryPojo>();

        while(nodeQuery.size()>1)
        {

            if(i==0)
            {
                i=1;
                childQuery.add(nodeQuery.get(0));
            }
            Iterator<NodeQueryPojo> iterQuery = childQuery.iterator();
            List<NodeQueryPojo> children = new LinkedList<NodeQueryPojo>();

            while (iterQuery.hasNext()) {

                NestedLoopsJoins nlj;

                nlj  = new Join().joinNodeSEdge(name, iterQuery.next());
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
            //            if(nodeQuery.size()==1)
            //                ansNids=children;
        }
        return ansNids;
    }
    
    

    public static List<EdgeQueryPojo> queryPE2(String name,List<EdgeQueryPojo> edgeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
    {
    	List<EdgeQueryPojo> ansNids= new LinkedList<EdgeQueryPojo>();
        int i=0;
        List<EdgeQueryPojo> childQuery = new LinkedList<EdgeQueryPojo>();
       while(edgeQuery.size()>1)
       {

           if(i==0)
           {
               i=1;
               
               childQuery.add(edgeQuery.get(0));
           }
           Iterator<EdgeQueryPojo> iterQuery = childQuery.iterator();
           List<EdgeQueryPojo> children = new LinkedList<EdgeQueryPojo>();
           while (iterQuery.hasNext()) {
        	   SortMerge smj;
        	   smj=new Join().joinEdgeEdge(name);
        	   Tuple t = new Tuple();
               iterator.Iterator joinResult=(iterator.Iterator)smj;
               while ((t = joinResult.get_next()) != null)
               {
            	   EdgeQueryPojo tmp=new EdgeQueryPojo();
            	   tmp.setDestLabel(t.getStrFld(8));
            	   NID destNid = new NID(); 
                   destNid.pageNo=new PageId(t.getIntFld(4));
                   destNid.slotNo=t.getIntFld(5);
                   
                   if(edgeQuery.get(1).getKey()==1)
                   {
                       String destlabel="";
                       destlabel =  SystemDefs.JavabaseDB.nhfile.getNode(destNid).getLabel();
                       //System.out.println("Destination: "+ t.getStrFld(8) +" Des: "+destDisc.getString2());
                       if(destlabel.equals(edgeQuery.get(1).getDestLabel())){
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
       }
        
        return null;
    }
    
    }


