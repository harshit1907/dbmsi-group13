package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.NID;
import global.PageId;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import iterator.DuplElim;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.SortMerge;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;
import queryPojo.Pair;

public class Query47 {

    public static List<EdgeQueryPojo> queryPE2(String name, List<EdgeQueryPojo> edgeQuery)
            throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException,
            SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        
        List<EdgeQueryPojo> ansNids = new LinkedList<EdgeQueryPojo>();
        List<EdgeQueryPojo> childQuery = new LinkedList<EdgeQueryPojo>();
        List<NodeQueryPojo> nchildQuery = new LinkedList<NodeQueryPojo>();
        int i = 0;

        while (edgeQuery.size() > 1) {
            List<EdgeQueryPojo> children = new LinkedList<EdgeQueryPojo>();
            if (i == 0) {
                NID nid = edgeQuery.get(0).getNd();
                String node_label = SystemDefs.JavabaseDB.nhfile.getNode(nid).getLabel();
                NodeQueryPojo npj = new NodeQueryPojo();
                npj.setLabel(node_label);
                nchildQuery.add(npj);
                Iterator<NodeQueryPojo> iterQuery = nchildQuery.iterator();
                while (iterQuery.hasNext()) {
                    NestedLoopsJoins nlj;

                    nlj = new Join().joinNodeSEdgeEid(name, iterQuery.next());
                    Tuple t = new Tuple();
                    iterator.Iterator joinResult = (iterator.Iterator) nlj;
                    while ((t = joinResult.get_next()) != null) {
                        EdgeQueryPojo tmp = new EdgeQueryPojo();
                        //System.out.println("sadasd"+t.getIntFld(10));
                        tmp.setUniqueKey(t.getIntFld(10));
                        NID destNid = new NID();
                        destNid.pageNo = new PageId(t.getIntFld(4));
                        destNid.slotNo = t.getIntFld(5);
                        String label = t.getStrFld(6);
                        int wt = t.getIntFld(1);
                        // System.out.println("__________"+edgeQuery.get(1).getKey());
                        if (edgeQuery.get(1).getKey() == 1) {
                            if (edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label)) {
                                children.add(tmp);
                                if (edgeQuery.size() == 2) {
                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    ansNids.add(destNidPojo);
                                }
                            }

                        } else if (edgeQuery.get(1).getKey() == 2) {
                            if (edgeQuery.get(1).getWeight() >= wt) {
                                children.add(tmp);
                                if (edgeQuery.size() == 2) {
                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    ansNids.add(destNidPojo);
                                }
                            }
                        }
                        joinResult.close();
                    }
                    nlj.close();
                    
                }

            } else {
                // System.out.println("__"+i);
                EdgeQueryPojo epj = new EdgeQueryPojo();
                Iterator<EdgeQueryPojo> iterQuery = childQuery.iterator();
                while (iterQuery.hasNext()) {
                    //System.out.println("$$$$$$$$$$$$$$$$$$$$");
                    EdgeQueryPojo edgeQueryPojo = iterQuery.next();
                    NestedLoopsJoins nlj = new Join().joinEdgeEdgeNested(name, edgeQueryPojo);
                    Tuple t = new Tuple();

                    while ((t = nlj.get_next()) != null) {
                        EdgeQueryPojo tmp = new EdgeQueryPojo();
                        int lab = t.getIntFld(9);
                        tmp.setUniqueKey(lab);
                        String label=t.getStrFld(6);
                        NID destNid = new NID();
                        //System.out.println("___________"+i);
                        //System.out.println(t.getStrFld(6));

                        destNid.pageNo = new PageId(t.getIntFld(4));
                        destNid.slotNo = t.getIntFld(5);
                        int wt = t.getIntFld(1);
                        if (edgeQuery.get(1).getKey() == 1) {
                            if (edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label)) {
                                children.add(tmp);
                                if (edgeQuery.size() == 2) {
                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    ansNids.add(destNidPojo);
                                }
                            }

                        } else if (edgeQuery.get(1).getKey() == 2) {
                            if (edgeQuery.get(1).getWeight() >= wt) {
                                children.add(tmp);
                                if (edgeQuery.size() == 2) {
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

            //System.out.println("children in iteration$$$$"+i);

            childQuery = children;
            //			
//            			  for(EdgeQueryPojo epj1:children) {
//            			  System.out.println(epj1.getEdgelabel()); }
            //			 
            edgeQuery.remove(0);
            i++;
        }
        //System.out.println("size of ansNids = "+ansNids.size());
        
//        for( EdgeQueryPojo tmpLi : ansNids)
//        {
//            System.out.println("(Page Number: "+tmpLi.getNd().pageNo+" | Slot Number: "+tmpLi.getNd().slotNo+"),");
//        }
        return ansNids;
        
    }
    //
    //	public static List<Pair<EdgeQueryPojo, EdgeQueryPojo>> queryPQ2a(String name, List<EdgeQueryPojo> edgeQuery,
    //			List<NodeQueryPojo> nodeQuery, String Type)
    //			throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException,
    //			SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    //
    //		NodeQueryPojo npj = nodeQuery.get(0);
    //		Iterator<NodeQueryPojo> iterQuery = nodeQuery.iterator();
    //		List<Pair<EdgeQueryPojo,EdgeQueryPojo>> ansNids= new ArrayList<Pair<EdgeQueryPojo,EdgeQueryPojo>>();
    //		List<Pair<EdgeQueryPojo,EdgeQueryPojo>> childQuery = new ArrayList<Pair<EdgeQueryPojo,EdgeQueryPojo>>();
    //		
    //        List<Pair<EdgeQueryPojo,EdgeQueryPojo>>  children = new ArrayList<Pair<EdgeQueryPojo,EdgeQueryPojo>>();
    //
    //		NodeQueryPojo nj=nodeQuery.get(0);
    //		
    //	        
    //        int i=0;
    //		while (iterQuery.hasNext()) {
    //			NestedLoopsJoins nlj;
    //			
    //			nlj = new Join().joinNodeSEdge(name, iterQuery.next());
    //			Tuple t = new Tuple();
    //			while ((t = nlj.get_next()) != null) {
    //				EdgeQueryPojo tmp = new EdgeQueryPojo();
    //				String label = t.getStrFld(6);
    //				tmp.setEdgelabel(label);
    //				NID destNid = new NID();
    //				destNid.pageNo = new PageId(t.getIntFld(4));
    //				destNid.slotNo = t.getIntFld(5);
    //				String dest_label=SystemDefs.JavabaseDB.nhfile.getNode(destNid).getLabel();
    //				tmp.setDestLabel(dest_label);		
    //				
    //				int wt = t.getIntFld(1);
    //				if (edgeQuery.get(0).getKey() == 1) {
    //					if (edgeQuery.get(0).getEdgelabel().equalsIgnoreCase(label)) {
    //						EdgeQueryPojo rtemp=new EdgeQueryPojo();
    //                        rtemp.setDestLabel(t.getStrFld(7));
    //						children.add(new Pair<EdgeQueryPojo,EdgeQueryPojo>(rtemp,tmp));
    //						if (edgeQuery.size() == 1) {
    //							EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
    //							destNidPojo.setDestLabel(dest_label);
    //							ansNids.add(new Pair<EdgeQueryPojo,EdgeQueryPojo>(rtemp,destNidPojo));
    //						} 
    //					}
    //					}
    //				else if (edgeQuery.get(0).getKey() == 2) {
    //					//System.out.println(wt+","+edgeQuery.get(0).getWeight());
    //					if (edgeQuery.get(0).getWeight() >= wt) {
    //						EdgeQueryPojo rtemp=new EdgeQueryPojo();
    //                        rtemp.setDestLabel(t.getStrFld(7));
    //						children.add(new Pair<EdgeQueryPojo,EdgeQueryPojo>(rtemp,tmp));
    //						if (edgeQuery.size() == 1) {
    //							EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
    //							destNidPojo.setNd(destNid);
    //							
    //							ansNids.add(new Pair<EdgeQueryPojo,EdgeQueryPojo>(rtemp,destNidPojo));
    //						}
    //					}
    //				}
    //			}
    //			nlj.close();
    //		}
    //			childQuery=children;
    //			
    //			edgeQuery.remove(0);
    //			while(edgeQuery.size()>=1)
    //	        {
    //				
    //	            List<Pair<NodeQueryPojo,NodeQueryPojo>>  children_nested = new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();
    //			
    //				
    //				EdgeQueryPojo epj = new EdgeQueryPojo();
    //				//System.out.println(childQuery.size());
    //				Iterator<Pair<EdgeQueryPojo, EdgeQueryPojo>> iterQueryEdge = childQuery.iterator();
    //				
    //				while (iterQueryEdge.hasNext()) {
    //					Pair<EdgeQueryPojo,EdgeQueryPojo> nextIterQuery = iterQueryEdge.next();
    //					NestedLoopsJoins nlj_inside = new Join().joinEdgeEdgeNested(name, nextIterQuery.getRight());
    //					Tuple t_inside = new Tuple();
    //					while ((t_inside = nlj_inside.get_next()) != null) {
    //						
    //						EdgeQueryPojo tmp = new EdgeQueryPojo();
    //						String label = t_inside.getStrFld(6);
    //						tmp.setEdgelabel(label);
    //						NID destNid = new NID();
    //						destNid.pageNo = new PageId(t_inside.getIntFld(4));
    //						destNid.slotNo = t_inside.getIntFld(5);
    //						NID srcNid = new NID();
    //						srcNid.pageNo = new PageId(t_inside.getIntFld(2));
    //						srcNid.slotNo = t_inside.getIntFld(3);
    //						int wt = t_inside.getIntFld(1);
    //						if (edgeQuery.get(0).getKey() == 1) {
    //							if (edgeQuery.get(0).getEdgelabel().equalsIgnoreCase(label)) {
    //								
    //								children_nested.add(tmp);
    //								if (edgeQuery.size() == 1) {
    //									EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
    //									destNidPojo.setNd(destNid);
    //									EdgeQueryPojo srcNidPojo = new EdgeQueryPojo();
    //									srcNidPojo.setNd(srcNid);
    //									ansNids.add(new Pair<EdgeQueryPojo,EdgeQueryPojo>(srcNidPojo,destNidPojo));
    //								}
    //							}
    //
    //						} else if (edgeQuery.get(0).getKey() == 2) {
    //							if (edgeQuery.get(0).getWeight() >= wt) {
    //								children_nested.add(tmp);
    //								if (edgeQuery.size() == 1) {
    //									EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
    //									destNidPojo.setNd(destNid);
    //									EdgeQueryPojo srcNidPojo = new EdgeQueryPojo();
    //									srcNidPojo.setNd(srcNid);
    //									ansNids.add(new Pair<EdgeQueryPojo,EdgeQueryPojo>(srcNidPojo,destNidPojo));
    //								}
    //							}
    //						}
    //
    //					}
    //					nlj_inside.close();
    //
    //				}
    //				edgeQuery.remove(0);
    //				childQuery=children_nested;
    ////				System.out.println("iterating  ");
    ////				for(EdgeQueryPojo ep:childQuery)
    ////				{
    ////					System.out.println(ep.getEdgelabel());
    ////				}
    //	        }
    //			
    //			/*System.out.println("iterating");
    //			
    //			for(EdgeQueryPojo ep:childQuery)
    //			{
    //				System.out.println(ep.getEdgelabel());
    //			}
    //			
    //			System.out.println("children");
    //			for(EdgeQueryPojo ep:children)
    //			{
    //				System.out.println(ep.getEdgelabel());
    //			}*/
    //			
    //		
    //
    //		return ansNids;
    //		// BC code
    //			
    //			
    //
    ////
    ////	        // CREATE Temp Heap File
    ////	        List<Pair<EdgeQueryPojo,EdgeQueryPojo>> ansNidsSorted = new ArrayList<Pair<EdgeQueryPojo,EdgeQueryPojo>>();
    ////	        Tuple nhp =new Tuple();
    ////	        AttrType[] attrType = new AttrType[1];
    ////	        attrType[0] = new AttrType(AttrType.attrString);
    ////
    ////	        short[] attrSize = new short[1];
    ////	        attrSize[0] = 20;
    ////	        nhp.setHdr((short)1, attrType, attrSize);
    ////
    ////	        Heapfile f = new Heapfile(null);
    ////	        for (int j=0;j<ansNids.size();j++) {
    ////	            nhp.setStrFld(1, String.format("%05d", Integer.parseInt(ansNids.get(j).getLeft().getLabel()))
    ////	                    + "_" + String.format("%05d", Integer.parseInt(ansNids.get(j).getRight().getLabel())));
    ////	            f.insertRecord(nhp.getTupleByteArray());
    ////	        }
    ////
    ////	        if (Type.equalsIgnoreCase("a")) {
    ////	         // clean up
    ////	            try {
    ////	                
    ////	                f.deleteFile();
    ////	            } catch (IOException e) {
    ////	                e.printStackTrace();
    ////	            }
    ////	            return ansNids;
    ////	        }
    ////	        
    ////	        // SORT the file just created
    ////	        FldSpec[] projlist = new FldSpec[1];
    ////	        RelSpec rel = new RelSpec(RelSpec.outer);
    ////	        projlist[0] = new FldSpec(rel, 1);
    ////	        TupleOrder[] order = new TupleOrder[1];
    ////	        order[0] = new TupleOrder(TupleOrder.Ascending);
    ////	        int numPages = 30;
    ////	        
    ////	        FileScan fscan = null;
    ////	        try {
    ////	            fscan = new FileScan(f._fileName, attrType, attrSize, (short) 1, 1, projlist, null);
    ////	        }
    ////	        catch (Exception e) {
    ////	            e.printStackTrace();
    ////	        }
    ////
    ////	        Sort sort = null;
    ////	        try {
    ////	            sort = new Sort(attrType, (short) 1, attrSize, fscan, 1, order[0], 20, numPages);
    ////	        }
    ////	        catch (Exception e) {
    ////	            e.printStackTrace();
    ////	        }
    ////	        
    ////	        if (Type.equalsIgnoreCase("b")) {
    ////	            int count = 0;
    ////	            Tuple t = null;
    ////	            String outval = null;
    ////
    ////	            try {
    ////	                t = sort.get_next();
    ////	            }
    ////	            catch (Exception e) {
    ////	                e.printStackTrace(); 
    ////	            }
    ////
    ////	            boolean flag = true;
    ////	            while (t != null) {                   
    ////	                try {
    ////	                    NodeQueryPojo head = new NodeQueryPojo();
    ////	                    NodeQueryPojo tail = new NodeQueryPojo();
    ////	                    head.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(0,5))));
    ////	                    tail.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(6,11))));
    ////	                    ansNidsSorted.add(new Pair<NodeQueryPojo,NodeQueryPojo>(head, tail));
    ////	                    t = sort.get_next();
    ////	                }
    ////	                catch (Exception e) {
    ////	                    e.printStackTrace();
    ////	                }
    ////	            }
    ////
    ////	            // clean up
    ////	            try {
    ////	                fscan.close();
    ////	                sort.close();
    ////	                f.deleteFile();
    ////	            } catch (IOException e) {
    ////	                e.printStackTrace();
    ////	            }
    ////	            return ansNidsSorted;
    ////	        }
    ////	        
    ////	        // Type = "c"
    ////	        DuplElim ed = null;
    ////	        try {
    ////	          ed = new DuplElim(attrType, (short)1, attrSize, sort, 10, true);
    ////	        }
    ////	        catch (Exception e) {
    ////	          System.err.println (""+e);
    ////	          Runtime.getRuntime().exit(1);
    ////	        }
    ////
    ////	        int count = 0;
    ////	        Tuple t = null;
    ////	        String outval = null;
    ////
    ////	        try {
    ////	            t = ed.get_next();
    ////	        }
    ////	        catch (Exception e) {
    ////	            e.printStackTrace(); 
    ////	        }
    ////
    ////	        boolean flag = true;
    ////	        while (t != null) {                   
    ////	            try {
    ////	                NodeQueryPojo head = new NodeQueryPojo();
    ////	                NodeQueryPojo tail = new NodeQueryPojo();
    ////	                head.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(0,5))));
    ////	                tail.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(6,11))));
    ////	                ansNidsSorted.add(new Pair<NodeQueryPojo,NodeQueryPojo>(head, tail));
    ////	                t = ed.get_next();
    ////	            }
    ////	            catch (Exception e) {
    ////	                e.printStackTrace();
    ////	            }
    ////	        }
    ////
    ////	        // clean up
    ////	        try {
    ////	            fscan.close();
    ////	            sort.close();
    ////	            ed.close();
    ////	        } catch (IOException e) {
    ////	            e.printStackTrace();
    ////	        }
    ////	        
    ////	        return ansNidsSorted;
    //
    //	}
    //
}
