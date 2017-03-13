package tests;
//originally from : joins.C


import com.sun.corba.se.impl.ior.NewObjectKeyTemplateBase;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import edgeheap.Edge;
import global.RID;
import global.AttrType;
import global.EID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

public class EdgeQuery
{
	  public final static boolean OK   = true; 
	  public final static boolean FAIL = false;
	public void edgeQuery(String graphDBName,int numBuf,int qType,int index,int queryOptions) throws HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		
		switch(qType)
		{
		case 0: 
			
			System.out.println(" query will print the edge data in the order it occurs in the edge heap");
			if(index==0) {
				if(SystemDefs.JavabaseDB!=null) 
					SystemDefs.JavabaseBM.flushAllPages();
				SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
				SystemDefs.JavabaseBM.flushAllPages();
				
				new FullScanEdge().fullScanEdge(graphDBName);
			} 
			else {
						}
			break;
				
			case 1:
				System.out.println(" query will print the edge data  in increasing alphanumerical order of source labels.");
	            break;
			case 2:
				System.out.println(" query will print the edge data in increasing alphanumerical order of destination labels");
			    break;
			case 3:
			    if(index==0) {
	                if(SystemDefs.JavabaseDB!=null) 
	                    SystemDefs.JavabaseBM.flushAllPages();
	                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                SystemDefs.JavabaseBM.flushAllPages();
	                
	                new FullScanEdge().fullScanEdge(graphDBName);
	            } 
	            else {
	                
	                if(SystemDefs.JavabaseDB!=null) 
	                    SystemDefs.JavabaseBM.flushAllPages();
	                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                //SystemDefs.JavabaseBM.flushAllPages();
	                
	                boolean status = OK;
	                // start index scan
	                BTFileScan iscan = null;
	                 SystemDefs.JavabaseDB.btEdgeLabel = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeEdgeIndex", AttrType.attrString, 32, 1/*delete*/);
	                    
	                try {
	                    System.out.println(SystemDefs.JavabaseDB.btEdgeLabel);
	                    
	                    iscan = SystemDefs.JavabaseDB.btEdgeLabel.new_scan(null, null);
	                    
	                }
	                catch (Exception e) {
	                    status = FAIL;
	                    e.printStackTrace();
	                }

	                KeyDataEntry t=null;
	                try {
	                    t = iscan.get_next();
	                }
	                catch (Exception e) {
	                    status = FAIL;
	                    e.printStackTrace();
	                }
	                boolean flag = true;
	                while (t != null && iscan!=null) {
	                    try {
	                        t = iscan.get_next();
	                    }
	                    catch (Exception e) {
	                        status = FAIL;
	                        e.printStackTrace();
	                    }
	                    
	                    try {
	                        if(t==null) break;
	                        StringKey k = (StringKey)t.key;
	                        LeafData l = (LeafData)t.data;
	                        RID rid =  l.getData();
	                        EID eid = new EID(rid.pageNo, rid.slotNo);
	                        Edge edge = SystemDefs.JavabaseDB.ehfile.getEdge(eid);
	                        System.out.println("Label: "+edge.getLabel()+" -- Weight: "+edge.getWeight());
	                    }
	                    catch (Exception e) {
	                        status = FAIL;
	                        e.printStackTrace();
	                    }

	                }

	                if (flag && status) {
	                    System.out.println("Test1 -- Index Scan OK");
	                }

	                // clean up
	                try {
	                    //iscan.close();
	                    SystemDefs.JavabaseDB.btEdgeLabel.close();
	                }
	                catch (Exception e) {
	                    status = FAIL;
	                    e.printStackTrace();
	                }
	            }

				 System.out.println("query will print the edge data in increasing alphanumerical order of edge labels.");
			     break;
			case 4:
	             if(index==0) {
	                    if(SystemDefs.JavabaseDB!=null) 
	                        SystemDefs.JavabaseBM.flushAllPages();
	                    SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                    SystemDefs.JavabaseBM.flushAllPages();
	                    
	                    new FullScanEdge().fullScanEdge(graphDBName);
	                } 
	                else {
	                    
	                    if(SystemDefs.JavabaseDB!=null) 
	                        SystemDefs.JavabaseBM.flushAllPages();
	                    SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                    //SystemDefs.JavabaseBM.flushAllPages();
	                    
	                    boolean status = OK;
	                    // start index scan
	                    BTFileScan iscan = null;
	                     SystemDefs.JavabaseDB.btEdgeWeight = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeEdgeIndex", AttrType.attrString, 32, 1/*delete*/);
	                        
	                    try {
	                        System.out.println(SystemDefs.JavabaseDB.btEdgeWeight);
	                        
	                        iscan = SystemDefs.JavabaseDB.btEdgeWeight.new_scan(null, null);
	                        
	                    }
	                    catch (Exception e) {
	                        status = FAIL;
	                        e.printStackTrace();
	                    }

	                    KeyDataEntry t=null;
	                    try {
	                        t = iscan.get_next();
	                    }
	                    catch (Exception e) {
	                        status = FAIL;
	                        e.printStackTrace();
	                    }
	                    boolean flag = true;
	                    while (t != null && iscan!=null) {
	                        try {
	                            t = iscan.get_next();
	                        }
	                        catch (Exception e) {
	                            status = FAIL;
	                            e.printStackTrace();
	                        }
	                        
	                        try {
	                            if(t==null) break;
	                            StringKey k = (StringKey)t.key;
	                            LeafData l = (LeafData)t.data;
	                            RID rid =  l.getData();
	                            EID eid = new EID(rid.pageNo, rid.slotNo);
	                            Edge edge = SystemDefs.JavabaseDB.ehfile.getEdge(eid);
	                            System.out.println("Label: "+edge.getLabel()+" -- Weight: "+edge.getWeight());
	                        }
	                        catch (Exception e) {
	                            status = FAIL;
	                            e.printStackTrace();
	                        }

	                    }

	                    if (flag && status) {
	                        System.out.println("Test1 -- Index Scan OK");
	                    }

	                    // clean up
	                    try {
	                        //iscan.close();
	                        SystemDefs.JavabaseDB.btEdgeWeight.close();
	                    }
	                    catch (Exception e) {
	                        status = FAIL;
	                        e.printStackTrace();
	                    }
	                }

			    
				 System.out.println("query will print the edge data in increasing order of weights");
			     break;
			case 5:
				System.out.println("query will take a lower and upper bound on edge weights, and will return the matching edge data");
				break;
			case 6:
				System.out.println(" query will return pairs of incident graph edges");
				break;
			default:
				System.out.println("invalid option");
				break;
			}


	}
	/*public static void main(String[] args) throws InvalidTupleSizeException, IOException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException
	{
		String graphDBName=args[0];
		int numBuf=Integer.parseInt(args[1]);
		int qType=Integer.parseInt(args[2]);
		int index=Integer.parseInt(args[3]);
		int queryOptions=Integer.parseInt(args[4]);
		//NodeQuery nodequery=new NodeQuery(graphDBName, numBuf,qType,index,queryOptions);
	}*/
}	