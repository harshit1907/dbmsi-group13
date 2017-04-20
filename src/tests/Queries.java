package tests;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.sun.org.apache.regexp.internal.RE;

import bufmgr.PageNotReadException;
import heap.Tuple;
import index.IndexException;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import queryPojo.NodeQueryPojo;

public class Queries {




    public static void queryPE1(String name,List<NodeQueryPojo> nodeQueryPojo, List<NodeQueryPojo> ansPojo) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
    {


        if(nodeQueryPojo.size()==1)
            return ;
        NestedLoopsJoins nlj;

        nlj  = new Join().joinNodeSEdge(name, nodeQueryPojo.get(0));
        Tuple t = new Tuple();
        iterator.Iterator children=(iterator.Iterator)nlj;

        //List<NodeQueryPojo> BFS = new LinkedList<NodeQueryPojo>();
        List<NodeQueryPojo> BFS = new LinkedList<NodeQueryPojo>();
        
        
        while ((t = children.get_next()) != null) {
            NodeQueryPojo tmp=new NodeQueryPojo();
            tmp.setLabel(t.getStrFld(7));
            BFS.add(tmp);
        }
        
        nlj.close();
        

        while(BFS.size()>0) {
            NodeQueryPojo current = BFS.get(0);
            
            NestedLoopsJoins nlj2;

            nlj2  = new Join().joinNodeSEdge(name, nodeQueryPojo.get(0));
            children=(iterator.Iterator)nlj2;
            
            
            while ((t = children.get_next()) != null) {


                //              //  System.out.println("Label:\t"+t.getStrFld(6)+ " "+t.getStrFld(7)+" "+t.getDescFld(9).getString());
                //              NodeQueryPojo tmp=new NodeQueryPojo();
                //              tmp.setLabel(t.getStrFld(7));
                //              NodeQueryPojo ansTmp=new NodeQueryPojo();
                //              ansTmp.setLabel(t.getStrFld(8));
                //                       boolean lastFlg = false;
                //                       if(nodeQueryPojo.get(1).getKey()==2)
                //                       {
                //                           if(t.getDescFld(9).equals(nodeQueryPojo.get(1).getDesc())){
                //                               nodeQueryPojo.add(tmp);
                //                               
                //                           }
                //                       }
                //                       else
                //                       {
                //                           if(t.getStrFld(7).equals(nodeQueryPojo.get(1).getLabel())){
                //                               nodeQueryPojo.add(tmp);
                //                           }
                //                       }  
                //              

            }
            nlj2.close();  

            //nodeQueryPojo.remove(0);
        }

        System.out.println("AYA ANDAAR " + nodeQueryPojo.size());

        queryPE1(name,nodeQueryPojo,ansPojo);

        return;
    }

}
