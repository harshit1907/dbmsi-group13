package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import edgeheap.Edge;
import global.EID;
import global.NID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

import nodeheap.NScan;
import nodeheap.Node;

public class BatchEdgeInsert {
	private final static boolean OK = true;
	private final static boolean FAIL = false;

	protected boolean batchEdgeInsert(String nodefilename) throws IOException, InvalidTupleSizeException, InvalidSlotNumberException {
		System.out.println("\n  Task 11: Batch Insert Edges\n");
		boolean status = OK;
		BufferedReader br = null;
		System.out.println("  - Create a edge heap file\n");
		int countE=0, countT=0;
		
		if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
			System.err.println("*** The heap file has left pages pinned\n");
			status = FAIL;
		}

		if (status == OK) {

			br = new BufferedReader(new FileReader(nodefilename));
			try {

				String line = br.readLine();
				while (line != null) {
					String[] strList = line.split(" ");
					line = br.readLine();
					countT++;
					Edge currEdge = new Edge();
					NID srcNid = SystemDefs.JavabaseDB.nhfile.getNID(strList[0]);
					NID destNid = SystemDefs.JavabaseDB.nhfile.getNID(strList[1]);
					if(srcNid!=null&&destNid!=null)
					{
						currEdge.setSource(srcNid);
						currEdge.setDestination(destNid);
						currEdge.setLabel(strList[2]);
						currEdge.setWeight(Integer.parseInt(strList[3]));

						try {
							EID eid = SystemDefs.JavabaseDB.ehfile.insertEdge(currEdge.getEdgeByteArray());
							countE++;

						} catch (Exception e) {
							status = FAIL;
							System.err.println("*** Error inserting record " + "\n");
							e.printStackTrace();
						}

						if (status == OK
								&& SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {

							System.err.println("*** Insertion left a page pinned\n");
							status = FAIL;
						}

						//System.out.println("");
					}
				}
			} finally {
				br.close();
			}
			System.out.println();
			  System.out.println("Inserted "+countE+" Edges. Edges given in file to insert "+countT);
		}
		System.out.println("Node Count: "+SystemDefs.JavabaseDB.getNodeCnt());

		System.out.println("Edge Count: "+SystemDefs.JavabaseDB.getEdgeCnt());

		System.out.println("Disk Read Count: "+PCounter.readCounter);

		System.out.println("Disk Write Count: "+PCounter.writeCounter);

		return true;
	}
}
