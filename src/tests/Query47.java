package tests;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import bufmgr.PageNotReadException;
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

public class Query47 {
	
	
    

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
                        //System.out.println("__________"+edgeQuery.get(1).getKey());
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
                    nlj.close();
                    
                    
                }
               
            }
            else{
            	
            	EdgeQueryPojo epj=new EdgeQueryPojo();
                Iterator<EdgeQueryPojo> iterQuery=childQuery.iterator();
                while (iterQuery.hasNext()) {
                	NestedLoopsJoins nlj=new  Join().joinEdgeEdgeNested(name, iterQuery.next());
                	Tuple t=new Tuple();
                	while((t=nlj.get_next())!=null)
                	{
                		EdgeQueryPojo tmp=new EdgeQueryPojo();
                		String label=t.getStrFld(6);
                		tmp.setEdgelabel(label);
                		NID destNid = new NID(); 
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
                	nlj.close();
                	
                }
                
            }
        
            //System.out.println("children in iteration"+i);
            
        
            childQuery=children;
            /*for(EdgeQueryPojo epj1:childQuery)
            {
                System.out.println(epj1.getEdgelabel());
            }*/
            edgeQuery.remove(0);
            i++;
        }
        
       
        return ansNids;
    }
 public static List<Pair<EdgeQueryPojo,EdgeQueryPojo>> queryPQ2a(String name,List<EdgeQueryPojo> edgeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
 {
     List<Pair<EdgeQueryPojo,EdgeQueryPojo>> ansNids= new ArrayList<Pair<EdgeQueryPojo,EdgeQueryPojo>>();
     int i=0;
     List<Pair<EdgeQueryPojo,EdgeQueryPojo>> childQuery = new ArrayList<Pair<EdgeQueryPojo,EdgeQueryPojo>>();
     while(edgeQuery.size()>1)
     {
    	 
     }
     
   return null;
 }

}
