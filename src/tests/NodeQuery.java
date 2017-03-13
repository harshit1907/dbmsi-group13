package tests;
//originally from : joins.C

import java.io.IOException;

import btree.AddFileEntryException;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import global.AttrType;
import global.NID;
import global.RID;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import nodeheap.Node;
import zbtree.*;

public class NodeQuery
{
	  public final static boolean OK   = true; 
	  public final static boolean FAIL = false;
	public void nodeQuery(String graphDBName,int numBuf,int qType,int index,int queryOptions) throws InvalidTupleSizeException, IOException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, GetFileEntryException, ConstructPageException, AddFileEntryException, zbtree.GetFileEntryException, zbtree.ConstructPageException, zbtree.AddFileEntryException
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
				
				
			}
			break;
		case 1:
			System.out.println("query will print the node data in increasing alphanumerical order of labels");
			break;
		case 2:
			System.out.println(" query will print the node data in increasing order of distance from a given 5D target descriptor");
			break;
		case 3:
			System.out.println("then the query will take a target descriptor and a distance and return the labels of nodes with the given distance from the target descripto");
			break;
		case 4:
			System.out.println("query will take a label and return all relevant information (including outgoing and incoming edges) about the node with the matching labe");
			break;
		case 5:
			System.out.println("query will take a target descriptor and a distance and return all relevant information (including outgoing and incoming edges) about the nodes with the given distance from the target descriptor.");
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