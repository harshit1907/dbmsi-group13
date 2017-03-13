package tests;
//originally from : joins.C



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import btree.AddFileEntryException;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IntegerKey;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.PCounter;
import edgeheap.EScan;
import edgeheap.Edge;
import global.AttrType;
import global.EID;
import global.NID;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import nodeheap.NScan;
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
				if(index==0) {
	                if(SystemDefs.JavabaseDB!=null) 
	                    SystemDefs.JavabaseBM.flushAllPages();
	                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                SystemDefs.JavabaseBM.flushAllPages();
	                
	                new FullScanEdge().fullScanEdge(graphDBName);
	            } 
	            else {
	            	String srcFileName = "_BTreeEdgeSourceIndex";
	                if(SystemDefs.JavabaseDB!=null) 
	                    SystemDefs.JavabaseBM.flushAllPages();
	                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                //SystemDefs.JavabaseBM.flushAllPages();
	                BTreeFile sourceLabelFile = new BTreeFile(SystemDefs.JavabaseDBName+srcFileName, AttrType.attrString, 32, 1/*delete*/);
	   	         sourceLabelFile.close();
	   	        
	                boolean status = OK;
	                // start index scan
	                BTFileScan iscan = null;
	                 SystemDefs.JavabaseDB.btEdgeLabel = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeEdgeIndex", AttrType.attrString, 32, 1/*delete*/);
	                    
	                try {
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
	                     		Node src = SystemDefs.JavabaseDB.nhfile.getNode(edge.getSource());
	                     		if(sourceLabelFile!=null)
	                     		{
	                     		sourceLabelFile = new BTreeFile(SystemDefs.JavabaseDBName+srcFileName, AttrType.attrString, 32, 1/*delete*/);
	    	        			sourceLabelFile.insert(new StringKey(src.getLabel()),eid);
	    	        			sourceLabelFile.close();
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
	                    SystemDefs.JavabaseDB.btEdgeLabel.close();
	                }
	                catch (Exception e) {
	                    status = FAIL;
	                    e.printStackTrace();
	                }
	               // System.out.println("Printing all alphanumerical sorted source labels");
	                new EdgeQuery().printLabelIndex(sourceLabelFile, srcFileName, graphDBName, numBuf);
	                
	                 
	            }

				break;
			case 2:
				System.out.println(" query will print the edge data in increasing alphanumerical order of destination labels");
				if(index==0) {
	                if(SystemDefs.JavabaseDB!=null) 
	                    SystemDefs.JavabaseBM.flushAllPages();
	                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                SystemDefs.JavabaseBM.flushAllPages();
	                
	                new FullScanEdge().fullScanEdge(graphDBName);
	            } 
	            else {
	        		String desFileName = "_BTreeEdgeDestinationIndex";
    	        	
	          	
	                if(SystemDefs.JavabaseDB!=null) 
	                    SystemDefs.JavabaseBM.flushAllPages();
	                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
	                //SystemDefs.JavabaseBM.flushAllPages();
	                BTreeFile destinationLabelFile = new BTreeFile(SystemDefs.JavabaseDBName+desFileName, AttrType.attrString, 32, 1/*delete*/);
	   	 	     destinationLabelFile.close();  	
	   	                 
	                boolean status = OK;
	                // start index scan
	                BTFileScan iscan = null;
	                 SystemDefs.JavabaseDB.btEdgeLabel = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeEdgeIndex", AttrType.attrString, 32, 1/*delete*/);
	                    
	                try {
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
	                        LeafData l = (LeafData)t.data;
	                        RID rid =  l.getData();
	                        EID eid = new EID(rid.pageNo, rid.slotNo);
	                        Edge edge = SystemDefs.JavabaseDB.ehfile.getEdge(eid);
	                     		Node des = SystemDefs.JavabaseDB.nhfile.getNode(edge.getDestination());
	                     		destinationLabelFile = new BTreeFile(SystemDefs.JavabaseDBName+desFileName, AttrType.attrString, 32, 1/*delete*/);
	    	        			destinationLabelFile.insert(new StringKey(des.getLabel()),eid);
	    	        			destinationLabelFile.close();
	    	         }
	                    catch (Exception e) {
	                        status = FAIL;
	                        e.printStackTrace();
	                    }

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
	               // System.out.println("Printing all alphanumerical sorted destination labels");
	                new EdgeQuery().printLabelIndex(destinationLabelFile, desFileName, graphDBName, numBuf);
	                
	                 
	            }
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
				if(index==0) {
					if(SystemDefs.JavabaseDB!=null) 
						SystemDefs.JavabaseBM.flushAllPages();
					SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
					SystemDefs.JavabaseBM.flushAllPages();
					
					NScan scan = null;
					boolean status = OK;

					if ( status == OK ) {	
					
						try {
							scan = SystemDefs.JavabaseDB.nhfile.openScan();
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
					NID nidTmp = new NID();

					if ( status == OK ) {
						Node node = null;

						boolean done = false;
						while (!done) { 
							node = scan.getNext(nidTmp);
							if (node == null) {
								done = true;
								break;
							}
							NID nid =SystemDefs.JavabaseDB.nhfile.getNID(node);
							List<EID> eidList = SystemDefs.JavabaseDB.ehfile.getEIDList(nid);
							List<Edge> listSource= new ArrayList<Edge>();
							List<Edge> listDes= new ArrayList<Edge>();
							if(eidList!=null&&eidList.size()>1)
							{
							for(EID i:eidList)
							{
								Edge curEdge = SystemDefs.JavabaseDB.ehfile.getEdge(i);
								NID nidSource =curEdge.getSource();
								NID nidDes = curEdge.getDestination();
								String nodeSource =  SystemDefs.JavabaseDB.nhfile.getNode(nidSource).getLabel();
								String nodeDes =  SystemDefs.JavabaseDB.nhfile.getNode(nidDes).getLabel();
								if(nodeSource.equalsIgnoreCase(node.getLabel()))   listSource.add(curEdge);
								if(nodeDes.equalsIgnoreCase(node.getLabel()))   listDes.add(curEdge);
							}
							System.out.println("Node Label: "+node.getLabel());
							if(listSource.size()>1&&listDes.size()>1)
							{
							for(Edge e:listSource)
							{
								for(Edge j:listDes)
								{
									System.out.println("Incident Pair: ["+e.getLabel()+" , "+j.getLabel()+" ]");
								}
							}
							}
							else
								System.out.println("No incident pairs!!");
							}
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
				SystemDefs.JavabaseDB.btNodeLabel = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeNodeIndex", AttrType.attrString, 32, 1/*delete*/);
				
				// start index scan
				BTFileScan iscan = null;
				try {
					iscan = SystemDefs.JavabaseDB.btNodeLabel.new_scan(null, null);
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
				//System.out.println(t+""+iscan);
				while (t != null && iscan!=null) {
			//System.out.println("hii");
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
						NID nid = new NID(rid.pageNo, rid.slotNo);
						Node node = SystemDefs.JavabaseDB.nhfile.getNode(nid);
						
						List<EID> eidList = SystemDefs.JavabaseDB.ehfile.getEIDList(nid);
						List<Edge> listSource= new ArrayList<Edge>();
						List<Edge> listDes= new ArrayList<Edge>();
						if(eidList!=null&&eidList.size()>1)
						{
						for(EID i:eidList)
						{
							Edge curEdge = SystemDefs.JavabaseDB.ehfile.getEdge(i);
							NID nidSource =curEdge.getSource();
							NID nidDes = curEdge.getDestination();
							String nodeSource =  SystemDefs.JavabaseDB.nhfile.getNode(nidSource).getLabel();
							String nodeDes =  SystemDefs.JavabaseDB.nhfile.getNode(nidDes).getLabel();
							if(nodeSource.equalsIgnoreCase(node.getLabel()))   listSource.add(curEdge);
							if(nodeDes.equalsIgnoreCase(node.getLabel()))   listDes.add(curEdge);
						}
						System.out.println("Node Label: "+node.getLabel());
						if(listSource.size()>1&&listDes.size()>1)
						{
						for(Edge e:listSource)
						{
							for(Edge j:listDes)
							{
								System.out.println("Incident Pair: ["+e.getLabel()+" , "+j.getLabel()+" ]");
							}
						}
						}
						else
							System.out.println("No incident pairs!!");
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
					SystemDefs.JavabaseDB.btNodeLabel.close();
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				}

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
	public void printLabelIndex(BTreeFile labelIndexFile, String nameOfFile,String graphDBName,int numBuf) throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException
	{
		if(SystemDefs.JavabaseDB!=null) 
            SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
        //SystemDefs.JavabaseBM.flushAllPages();
        
        boolean status = OK;
        // start index scan
        BTFileScan iscan = null;
       // System.out.println(SystemDefs.JavabaseDBName+nameOfFile);
        labelIndexFile = new BTreeFile(SystemDefs.JavabaseDBName+nameOfFile, AttrType.attrString, 32, 1/*delete*/);
            
        try {
             
            iscan = labelIndexFile.new_scan(null, null);
            
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
                System.out.println("Node Label: "+k.getKey()+" Edge Label: "+edge.getLabel()+" -- Weight: "+edge.getWeight());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

        }

        // clean up
        try {
            //iscan.close();
           labelIndexFile.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
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