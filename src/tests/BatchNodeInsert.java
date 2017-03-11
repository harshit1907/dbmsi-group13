package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import global.Descriptor;
import global.NID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import nodeheap.NScan;
import nodeheap.Node;

public class BatchNodeInsert
{
	 private final static boolean OK = true;
	  private final static boolean FAIL = false;
	  
	  private final static int reclen = 32;
	  

	  protected  boolean batchNodeInsert (String nodefilename,String graphDbName) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
		boolean status = OK;
	    BufferedReader br = null ;
	    int insertA=0,insertI=0;
	   
	    if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
		 != SystemDefs.JavabaseBM.getNumBuffers() ) {
	      System.err.println ("*** The heap file has left pages pinned\n");
	      status = FAIL;
	    }

	    if ( status == OK ) {
	    	
	    	try {
	    		br= new BufferedReader(new FileReader(nodefilename));
	    	    		
	    	    String line = br.readLine();
	    	    while (line != null) {
	    	    insertA++;
	    	    String[] strList=line.split(" ");
	    	    line = br.readLine();
	            
	            Node currentNode =  new Node();
	    		
	            Descriptor desc = new Descriptor();
	    		String label = strList[0];
	    		desc.set(Integer.parseInt(strList[1]),Integer.parseInt(strList[2]),Integer.parseInt(strList[3]),Integer.parseInt(strList[4]),Integer.parseInt(strList[5]));
	    		
	    		currentNode.setDesc(desc);
	    		currentNode.setLabel(label);
	    		
	    		try {
	    		
	    			NID  nid = SystemDefs.JavabaseDB.nhfile.insertNode(currentNode.getNodeByteArray());
	    			insertI++;
	    		}
	    		catch (Exception e) {
	    		  status = FAIL;
	    		  System.err.println ("*** Error inserting record " + "\n");
	    		  return false;
	    		}
	    		
	    		
	    		

	    		if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
	    		     != SystemDefs.JavabaseBM.getNumBuffers() ) {
	    		  
	    		  System.err.println ("*** Insertion left a page pinned\n");
	    		  status = FAIL;
	    		}

	            
	            
	    	    }
	    	    
	    	    
	    	    
	    	    
	    	} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} finally {
	    	    try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
	    	    
	    	}	
	    }
	  System.out.println();
	  System.out.println("Inserted "+insertI+" Nodes. Nodes given in file to insert "+insertA);
	//new FullScanNode().fullScanNode(graphDbName);
	
	      

	    
	    return true;
	  }


}
