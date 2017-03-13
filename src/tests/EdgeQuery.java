package tests;
//originally from : joins.C



import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.ls.LSException;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import diskmgr.PCounter;
import edgeheap.EScan;
import edgeheap.Edge;
import global.AttrType;
import global.EID;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import nodeheap.Node;

public class EdgeQuery
{
	  public final static boolean OK   = true; 
	  public final static boolean FAIL = false;
	public void edgeQuery(String graphDBName,int numBuf,int qType,int index,String queryOptions) throws HFException, HFDiskMgrException, HFBufMgrException, Exception
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
				System.out.println("No Index!!");
						}
			break;
				
			case 1:
				System.out.println(" query will print the edge data  in increasing alphanumerical order of source labels.");
	            break;
			case 2:
				System.out.println(" query will print the edge data in increasing alphanumerical order of destination labels");
			    break;
			case 3:
				 System.out.println("query will print the edge data in increasing alphanumerical order of edge labels.");
					
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

			     break;
			case 4:

			    
				 System.out.println("query will print the edge data in increasing order of weights");

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
	                     SystemDefs.JavabaseDB.btEdgeWeight = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeEdgeWeightIndex", AttrType.attrInteger, 32, 1/*delete*/);
	                        
	                    try {
	                        
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
	                            IntegerKey k = (IntegerKey)t.key;
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
			     break;
			case 5:
				System.out.println("query will take a lower and upper bound on edge weights, and will return the matching edge data");
				String  token[] = queryOptions.split(" ");
				int lowerBound  =Integer.parseInt(token[0]);
				int  upperBound =Integer.parseInt(token[1]);
				List<EID> listEid=new ArrayList<EID>();
				if(index==0) {
	                    if(SystemDefs.JavabaseDB!=null) 
	                        SystemDefs.JavabaseBM.flushAllPages();
	                    SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                    SystemDefs.JavabaseBM.flushAllPages();
	                    
	                	EScan scan = null;
	            		boolean status = OK;
	            		
	            		if ( status == OK ) {	
	            			System.out.println ("  - Scan the records just inserted\n");

	            			try {
	            				scan = SystemDefs.JavabaseDB.ehfile.openScan();
	            			}
	            			catch (Exception e) {
	            				status = FAIL;
	            				System.err.println ("*** Error opening scan\n");
	            				e.printStackTrace();
	            			}

	            			if ( status == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
	            					== SystemDefs.JavabaseBM.getNumBuffers() ) {
	            				System.err.println ("*** The heap-file scan has not pinned the first page\n");
	            				status = FAIL;
	            			}
	            		}
	            		EID eidTmp = new EID();

	            		if ( status == OK ) {
	            			Edge edge = null;

	            			boolean done = false;
	            			while (!done) { 
	            				edge = scan.getNext(eidTmp);
	            				if (edge == null) {
	            					done = true;
	            					break;
	            				}
	            				  if(edge.getWeight()>=lowerBound&&edge.getWeight()<=upperBound)
	            				  {       
	            			listEid.add(eidTmp);	  }
	            				  }
	            		}
	            		scan.closescan();

	                } 
	                else {
	                    
	                    if(SystemDefs.JavabaseDB!=null) 
	                        SystemDefs.JavabaseBM.flushAllPages();
	                    SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                    //SystemDefs.JavabaseBM.flushAllPages();
	                    
	                    boolean status = OK;
	                    // start index scan
	                    BTFileScan iscan = null;
	                     SystemDefs.JavabaseDB.btEdgeWeight = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeEdgeWeightIndex", AttrType.attrInteger, 32, 1/*delete*/);
	                        
	                    try {
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
	                            IntegerKey k = (IntegerKey)t.key;
	                            Integer curweight = k.getKey();
	                            if(lowerBound<=curweight&&curweight<=upperBound)
	                            {
	                                
	                            	LeafData l = (LeafData)t.data;
	                            RID rid =  l.getData();
	                            EID eid = new EID(rid.pageNo, rid.slotNo);
	                            listEid.add(eid);
	                            }
	                        }
	                        catch (Exception e) {
	                            status = FAIL;
	                            e.printStackTrace();
	                        }

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
				if(!listEid.isEmpty())
				{
					System.out.println("Edges matched:"+listEid.size());
				for(EID i:listEid)
				{
				 Edge edge = SystemDefs.JavabaseDB.ehfile.getEdge(i);
                 Node src = SystemDefs.JavabaseDB.nhfile.getNode(edge.getSource());
                 Node des = SystemDefs.JavabaseDB.nhfile.getNode(edge.getDestination());
                 System.out.println("Source: "+src.getLabel()+" Destination: "+des.getLabel()+" Label: "+edge.getLabel()+" -- Weight: "+edge.getWeight());
				}
				}
				else
					System.out.println("No matching edges found!!");
				break;
			case 6:
				System.out.println(" query will return pairs of incident graph edges");
				break;
			default:
				System.out.println("invalid option");
				break;
			}
		System.out.println("");
		System.out.println("Node Count: "+SystemDefs.JavabaseDB.getNodeCnt());

		  System.out.println("Edge Count: "+SystemDefs.JavabaseDB.getEdgeCnt());

		  System.out.println("Disk Read Count: "+PCounter.readCounter);
		  
		  System.out.println("Disk Write Count: "+PCounter.writeCounter);



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