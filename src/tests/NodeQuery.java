package tests;
//originally from : joins.C

import java.util.ArrayList;
import java.util.List;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import diskmgr.PCounter;
import edgeheap.Edge;
import global.AttrType;
import global.EID;
import global.NID;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.Tuple;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.NodeFileScan;
import iterator.RelSpec;
import iterator.Sort;
import nodeheap.Node;

public class NodeQuery
{
    public final static boolean OK   = true; 
    public final static boolean FAIL = false;
    public void nodeQuery(String graphDBName,int numBuf,int qType,int index,String queryOptions) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
    {

        switch(qType)
        {
        case 0: 

            System.out.println("  query will print the node data in the order it occurs in the node heap");
            if(index==0) {
                if(SystemDefs.JavabaseDB!=null) 
                    SystemDefs.JavabaseBM.flushAllPages();
                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
                SystemDefs.JavabaseBM.flushAllPages();

                new FullScanNode().fullScanNode(graphDBName);
            } 
            else {
                System.out.println("No index");			
            }
            //				
            //				System.out.println("*******************************************************************\n\n\n");
            //				
            //				status = OK;
            //				SystemDefs.JavabaseDB.ztNodeDesc = new ZBTreeFile(SystemDefs.JavabaseDBName+"_ZTreeNodeIndex", AttrType.attrString, 180, 1/*delete*/);
            //				
            //				// start index scan
            //				ZBTFileScan izscan = null;
            //				try {
            //					izscan = SystemDefs.JavabaseDB.ztNodeDesc.new_scan(null, null);
            //				}
            //				catch (Exception e) {
            //					status = FAIL;
            //					e.printStackTrace();
            //				}
            //
            //				DescriptorDataEntry tz=null;
            //				try {
            //					tz = izscan.get_next();
            //				}
            //				catch (Exception e) {
            //					status = FAIL;
            //					e.printStackTrace();
            //				}
            //				flag = true;
            //				//System.out.println(t+""+iscan);
            //				while (tz != null && izscan!=null) {
            //			//System.out.println("hii");
            //					try {
            //						tz = izscan.get_next();
            //					}
            //					catch (Exception e) {
            //						status = FAIL;
            //						e.printStackTrace();
            //					}
            //					
            //					try {
            //						if(tz==null) break;
            //						DescriptorKey k = (DescriptorKey)tz.key;
            //						
            //						zbtree.LeafData l = (zbtree.LeafData)tz.data;
            //						NID nid =  l.getData();
            //						Node node = SystemDefs.JavabaseDB.nhfile.getNode(nid);
            //						System.out.println("Key: "+k.getKey()+" Label: "+node.getLabel()+" -- Descripotr: "+node.getDesc().value[0]+" "+node.getDesc().value[1]+" "+node.getDesc().value[2]+" "+node.getDesc().value[3]+" "+node.getDesc().value[4]);
            //					}
            //					catch (Exception e) {
            //						status = FAIL;
            //						e.printStackTrace();
            //					}
            //				}
            //
            //				if (flag && status) {
            //					System.out.println("Test1 -- Index Scan OK");
            //				}
            //
            //				// clean up
            //				try {
            //					//iscan.close();
            //					SystemDefs.JavabaseDB.ztNodeDesc.close();
            //				}
            //				catch (Exception e) {
            //					status = FAIL;
            //					e.printStackTrace();
            //				}



            break;
        case 1:
            System.out.println("query will print the node data in increasing alphanumerical order of labels");
            if(index==0) {
                if(SystemDefs.JavabaseDB!=null) 
                    SystemDefs.JavabaseBM.flushAllPages();
                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
                SystemDefs.JavabaseBM.flushAllPages();

                boolean status = OK;

                AttrType[] attrType = new AttrType[2];
                attrType[0] = new AttrType(AttrType.attrDesc);
                attrType[1] = new AttrType(AttrType.attrString);
                short[] attrSize = new short[1];
                attrSize[0] = 20; //REC_LEN1;
                TupleOrder[] order = new TupleOrder[1];
                order[0] = new TupleOrder(TupleOrder.Ascending);
                //order[1] = new TupleOrder(TupleOrder.Descending);
                
                // create an iterator by open a file scan
                FldSpec[] projlist = new FldSpec[2];
                RelSpec rel = new RelSpec(RelSpec.outer); 
                projlist[0] = new FldSpec(rel, 1);
                projlist[1] = new FldSpec(rel, 2);

                FileScan fscan = null;

                try {
                    fscan = new FileScan(SystemDefs.JavabaseDBName+"_Node", attrType, attrSize, (short) 2, 2, projlist, null);
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                // Sort 
                Sort sort = null;
                try {
                    sort = new Sort(attrType, (short) 2, attrSize, fscan, 2, order[0], 20, 30);
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }


                int count = 0;
                Tuple t = null;
                String outval = null;

                try {
                    t = sort.get_next();
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace(); 
                }

                boolean flag = true;

                while (t != null) {
                    
                    try 
                    {
                        //System.out.println("HI: "+t.getStrFld(1));
                        //System.out.println(t.getStrFld(2));
                    }
                    catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }

                    try {
                        
                        Node node =new Node ();
                        node.nodeInit(t.getTupleByteArray(), 0);
                        System.out.print("Label:\t"+node.getLabel());
                        System.out.println("\tDescriptor: "+node.getDesc().getString());
                        t = sort.get_next();
                    }
                    catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                

                // clean up
                try {
                    //sort.close();
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

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
                        System.out.println("Label: "+node.getLabel()+" -- Descripotr: "+node.getDesc().value[0]+" "+node.getDesc().value[1]+" "+node.getDesc().value[2]+" "+node.getDesc().value[3]+" "+node.getDesc().value[4]);
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
                    SystemDefs.JavabaseDB.btNodeLabel.close();
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }

            break;
        case 2:
            System.out.println(" query will print the node data in increasing order of distance from a given 5D target descriptor");
            break;
        case 3:
            System.out.println("then the query will take a target descriptor and a distance and return the labels of nodes with the given distance from the target descripto");
            break;
        case 4:
            System.out.println("query will take a label and return all relevant information (including outgoing and incoming edges) about the node with the matching label");
            List<EID> listEid = new ArrayList<EID>();
            Node matchNode = null;
            if(index==0) {
                if(SystemDefs.JavabaseDB!=null) 
                    SystemDefs.JavabaseBM.flushAllPages();
                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
                SystemDefs.JavabaseBM.flushAllPages();

                NID nid =SystemDefs.JavabaseDB.nhfile.getNID(queryOptions);
                if(nid!=null)	
                { listEid= SystemDefs.JavabaseDB.ehfile.getEIDList(nid);
                matchNode = SystemDefs.JavabaseDB.nhfile.getNode(nid);
                }
            } 
            else {
                if(SystemDefs.JavabaseDB!=null) 
                    SystemDefs.JavabaseBM.flushAllPages();
                SystemDefs sysdef = new SystemDefs(graphDBName,0,numBuf,"Clock",0);
                SystemDefs.JavabaseBM.flushAllPages();

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
                        NID nid = new NID(rid.pageNo, rid.slotNo);
                        Node node = SystemDefs.JavabaseDB.nhfile.getNode(nid);
                        if(node.getLabel().equalsIgnoreCase(queryOptions))
                        {
                            listEid= SystemDefs.JavabaseDB.ehfile.getEIDList(nid);
                            matchNode = node;
                        }

                    }
                    catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }
                try {
                    //iscan.close();
                    SystemDefs.JavabaseDB.btNodeLabel.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }


            }

            // clean up


            if(matchNode!=null&&!listEid.isEmpty())
            {
                System.out.println("Match Found: ");
                System.out.println("Node Label:"+ matchNode.getLabel());
                System.out.println("Node Descriptor: "+ matchNode.getDesc().value[0]+" "+matchNode.getDesc().value[1]+" "+ matchNode.getDesc().value[2]+" "+matchNode.getDesc().value[3]+" "+matchNode.getDesc().value[4]);

                List<Edge> listSource= new ArrayList<Edge>();
                List<Edge> listDes= new ArrayList<Edge>();
                for(EID i:listEid)
                {
                    Edge curEdge = SystemDefs.JavabaseDB.ehfile.getEdge(i);
                    NID nidSource =curEdge.getSource();
                    NID nidDes = curEdge.getDestination();
                    String nodeSource =  SystemDefs.JavabaseDB.nhfile.getNode(nidSource).getLabel();
                    String nodeDes =  SystemDefs.JavabaseDB.nhfile.getNode(nidDes).getLabel();
                    if(nodeSource.equalsIgnoreCase(queryOptions))   listSource.add(curEdge);
                    if(nodeDes.equalsIgnoreCase(queryOptions))   listDes.add(curEdge);
                }
                if(!listDes.isEmpty())
                {
                    System.out.println("Node Incomming Edges: "+listDes.size());
                    for(Edge e:listDes)
                    {
                        String src= SystemDefs.JavabaseDB.nhfile.getNode(e.getSource()).getLabel();
                        System.out.println("Source Node: "+src+" Label: "+e.getLabel() + " Weight: "+e.getWeight());
                    }
                }
                else
                    System.out.println("Node Incomming Edges: "+listDes.size());
                if(!listSource.isEmpty())
                {
                    System.out.println("Node Outgoing Edges: "+listSource.size());
                    for(Edge e:listSource)
                    {
                        String des= SystemDefs.JavabaseDB.nhfile.getNode(e.getDestination()).getLabel();
                        System.out.println("Destination Node: "+des+" Label: "+e.getLabel() + " Weight: "+e.getWeight());
                    }
                }
                else 	System.out.println("Node Outgoing Edges: "+listSource.size());

            }
            else 
                System.out.println("No mactch found!!");
            break;
        case 5:
            System.out.println("query will take a target descriptor and a distance and return all relevant information (including outgoing and incoming edges) about the nodes with the given distance from the target descriptor.");
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