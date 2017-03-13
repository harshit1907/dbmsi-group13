package nodeheap;

import static global.GlobalConst.INVALID_PAGE;
import static nodeheap.Filetype.ORDINARY;
import static nodeheap.Filetype.TEMP;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;

import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.AttrType;

import global.NID;
import global.PageId;
import global.SystemDefs;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidUpdateException;
import heap.SpaceNotAvailableException;
import zbtree.*;


/**
 * Created by prakhar on 2/18/17.
 */
interface  Filetype {
	int TEMP = 0;
	int ORDINARY = 1;

} // end of Filetype

public class NodeHeapFile {
	PageId _firstDirPageId;   // page number of header page
	int _ftype;
	private boolean _file_deleted;
	private String _fileName;
	private static int tempfilecount = 0;
	private static final int REC_LEN1 = 32;

	private NHFPage _newDatapage(DataPageInfo dpinfop)
			throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		Page apage = new Page();
		PageId pageId = new PageId();
		pageId = newPage(apage, 1);

		if(pageId == null) throw new HFException(null, "can't new pae");
		// initialize internal values of the new page:

		NHFPage nhfpage = new NHFPage();
		nhfpage.init(pageId, apage);

		dpinfop.pageId.pid = pageId.pid;
		dpinfop.recct = 0;
		dpinfop.availspace = nhfpage.available_space();

		return nhfpage;
	}

	/**
	 * Initialize.  A null name produces a temporary heapfile which will be
	 * deleted by the destructor.  If the name already denotes a file, the
	 * file is opened; otherwise, a new empty file is created.
	 *
	 * @param name
	 * @throws HFException        heapfile exception
	 * @throws HFBufMgrException  exception thrown from bufmgr layer
	 * @throws HFDiskMgrException exception thrown from diskmgr layer
	 * @throws IOException        I/O errors
	 */
	public NodeHeapFile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		_file_deleted = true;
		_fileName = null;

		if(name == null) {
			// If the name is NULL, allocate a temporary name
			// and no logging is required.
			_fileName = "tempHeapFile";
			String useId = new String("user.name");
			String userAccName;
			userAccName = System.getProperty(useId);
			_fileName = _fileName + userAccName;

			String filenum = Integer.toString(tempfilecount);
			_fileName = _fileName + filenum;
			_ftype = TEMP;
			tempfilecount ++;
		} else {
			_fileName = name;
			_ftype = ORDINARY;
		}

		// The constructor gets run in two different cases.
		// In the first case, the file is new and the header page
		// must be initialized.  This case is detected via a failure
		// in the db->get_file_entry() call.  In the second case, the
		// file already exists and all that must be done is to fetch
		// the header page into the buffer pool

		// try to open the file

		Page apage = new Page();
		_firstDirPageId = null;
		if (_ftype == ORDINARY)
			_firstDirPageId = get_file_entry(_fileName);

		if(_firstDirPageId==null) {
			// file doesn't exist. First create it.
			_firstDirPageId = newPage(apage, 1);
			// check error
			if(_firstDirPageId == null) throw new HFException(null, "can't new page");

			add_file_entry(_fileName, _firstDirPageId);
			// check error(new exception: Could not add file entry

			NHFPage firstDirPage = new NHFPage();
			firstDirPage.init(_firstDirPageId, apage);
			PageId pageId = new PageId(INVALID_PAGE);

			firstDirPage.setNextPage(pageId);
			firstDirPage.setPrevPage(pageId);
			unpinPage(_firstDirPageId, true /*dirty*/ );
			_file_deleted = false;
			// ASSERTIONS:
			// - ALL private data members of class Heapfile are valid:
			//
			//  - _firstDirPageId valid
			//  - _fileName valid
			//  - no datapage pinned yet
		}
	}

	public NID getNID(String Nd) throws IOException
	{
		boolean OK = true;
		boolean FAIL = false;
		NScan scan = null;
		boolean status = OK;
		NID nid = new NID();
		int found=0;
		if (status == OK) {
			try {
				scan = SystemDefs.JavabaseDB.nhfile.openScan();

			} catch (Exception e) {
				status = FAIL;
				System.err.println("*** Error opening scan\n");
				e.printStackTrace();
				return nid;
			}
		}
		if (status == OK) {
			Node node = new Node();
			boolean done = false;

			while (!done) {
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
					}
					//System.out.println(nid+"   "+SystemDefs.JavabaseDB.nhfile.getNode(nid));

				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();

				}

				//System.out.println("Deleting this .."+node.getLabel()+" ^^^ "+nodelabel);

				if (!done && status == OK) {
					if (Nd.equals(node.getLabel()))
					{
						done = true;
						found=1;
						//System.out.print("Deleting this .."+nodelabel);
					}
				}
			}
		}
		scan.closescan();
		if(found==1)
			return nid;
		else
			return null;
	}

	public NID getNID(Node Nd) throws IOException
	{
		boolean OK = true;
		boolean FAIL = false;
		NScan scan = null;
		boolean status = OK;
		NID nid = new NID();
		int found=0;
		if (status == OK) {
			try {
				scan = SystemDefs.JavabaseDB.nhfile.openScan();

			} catch (Exception e) {
				status = FAIL;
				System.err.println("*** Error opening scan\n");
				e.printStackTrace();
				return nid;
			}
		}
		if (status == OK) {
			Node node = new Node();
			boolean done = false;

			while (!done) {
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
					}
					//System.out.println(nid+"   "+SystemDefs.JavabaseDB.nhfile.getNode(nid));

				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();

				}

				//System.out.println("Deleting this .."+node.getLabel()+" ^^^ "+nodelabel);

				if (!done && status == OK) {
					if (Nd.getLabel().equals(node.getLabel()))
					{
						done = true;
						found=1;
						//System.out.print("Deleting this .."+nodelabel);
					}
				}
			}
		}
		scan.closescan();
		if(found==1)
			return nid;
		else
			return null;
	}
	/* Internal HeapFile function (used in getNode and updateRecord):
       returns pinned directory page and pinned data page of the specified
       user record(nid) and true if record is found.
       If the user record cannot be found, return false.
	 */
	@SuppressWarnings("unused")
	private boolean  _findDataPage(NID nid, PageId dirPageId, NHFPage dirpage,
			PageId dataPageId, NHFPage datapage, NID npDataPageNid)
					throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
					HFBufMgrException, HFDiskMgrException, Exception {

		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		NHFPage currentDirPage = new NHFPage();
		NHFPage currentDataPage = new NHFPage();
		NID currentDataPageNid = new NID();
		PageId nextDirPageId = new PageId();
		// datapageId is stored in dpinfo.pageId


		pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

		Node anode = new Node();

		while (currentDirPageId.pid != INVALID_PAGE) {
			for(currentDataPageNid = currentDirPage.firstNode();
					currentDataPageNid != null;
					currentDataPageNid = currentDirPage.nextNode(currentDataPageNid)) {
				try {
					anode = currentDirPage.getNode(currentDataPageNid);
				} catch (InvalidSlotNumberException e) {// check error! return false(done)
					return false;
				}

				DataPageInfo dpinfo = new DataPageInfo(anode);
				try{
					pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);


					//check error;need unpin currentDirPage
				} catch (Exception e) {
					unpinPage(currentDirPageId, false/*undirty*/);
					dirpage = null;
					datapage = null;
					throw e;
				}

				// ASSERTIONS:
				// - currentDataPage, currentDataPageNid, dpinfo valid
				// - currentDataPage pinned

				if(dpinfo.pageId.pid==nid.pageNo.pid)
				{
					anode = currentDataPage.returnNode(nid);
					// found user's record on the current datapage which itself
					// is indexed on the current dirpage.  Return both of these.

					dirpage.setpage(currentDirPage.getpage());
					dirPageId.pid = currentDirPageId.pid;

					datapage.setpage(currentDataPage.getpage());
					dataPageId.pid = dpinfo.pageId.pid;

					npDataPageNid.pageNo.pid = currentDataPageNid.pageNo.pid;
					npDataPageNid.slotNo = currentDataPageNid.slotNo;
					return true;
				} else {
					// user record not found on this datapage; unpin it
					// and try the next one
					unpinPage(dpinfo.pageId, false /*undirty*/);
				}
			}

			// if we would have found the correct datapage on the current
			// directory page we would have already returned.
			// therefore:
			// read in next directory page:

			nextDirPageId = currentDirPage.getNextPage();
			try{
				unpinPage(currentDirPageId, false /*undirty*/);
			} catch(Exception e) {
				throw new HFException (e, "heapfile,_find,unpinpage failed");
			}

			currentDirPageId.pid = nextDirPageId.pid;
			if(currentDirPageId.pid != INVALID_PAGE) {
				pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
				if(currentDirPage == null)
					throw new HFException(null, "pinPage return null page");
			}

		} // end of While01
		// checked all dir pages and all data pages; user record not found:(

		dirPageId.pid = dataPageId.pid = INVALID_PAGE;

		return false;
	}

	/** Return number of nodes in node heap file.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 */
	public int getNodeCnt()
			throws InvalidSlotNumberException,
			InvalidTupleSizeException,
			HFDiskMgrException,
			HFBufMgrException,
			IOException {
		int answer = 0;
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);

		NHFPage currentDirPage = new NHFPage();
		Page pageinbuffer = new Page();

		while(currentDirPageId.pid != INVALID_PAGE)
		{
			pinPage(currentDirPageId, currentDirPage, false);

			NID nid = new NID();
			Node anode;
			for (nid = currentDirPage.firstNode(); nid != null; nid = currentDirPage.nextNode(nid)) {
				anode = currentDirPage.getNode(nid);
				DataPageInfo dpinfo = new DataPageInfo(anode);
				answer += dpinfo.recct;
			}

			// ASSERTIONS: no more record
			// - we have read all datapage records on
			//   the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			unpinPage(currentDirPageId, false /*undirty*/);
			currentDirPageId.pid = nextDirPageId.pid;
		}

		return answer;
	} // end of getRecCnt


	/** Insert record into file, return its Rid.
	 *
	 * @param recPtr pointer of the record
	 * @param recLen the length of the record
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception SpaceNotAvailableException no space left
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException I/O errors
	 *
	 * @return the rid of the record
	 * @throws InsertException 
	 * @throws LeafDeleteException 
	 * @throws IteratorException 
	 * @throws IndexSearchException 
	 * @throws DeleteRecException 
	 * @throws ConvertException 
	 * @throws NodeNotMatchException 
	 * @throws PinPageException 
	 * @throws UnpinPageException 
	 * @throws ConstructPageException 
	 * @throws IndexInsertRecException 
	 * @throws LeafInsertRecException 
	 * @throws KeyNotMatchException 
	 * @throws KeyTooLongException 
	 * @throws AddFileEntryException 
	 * @throws GetFileEntryException 
	 * @throws ReplacerException 
	 * @throws HashEntryNotFoundException 
	 * @throws InvalidFrameNumberException 
	 * @throws PageUnpinnedException 
	 * @throws zbtree.KeyTooLongException 
	 * @throws zbtree.KeyNotMatchException 
	 * @throws zbtree.LeafInsertRecException 
	 * @throws zbtree.IndexInsertRecException 
	 * @throws zbtree.ConstructPageException 
	 * @throws zbtree.UnpinPageException 
	 * @throws zbtree.PinPageException 
	 * @throws zbtree.NodeNotMatchException 
	 * @throws zbtree.ConvertException 
	 * @throws zbtree.DeleteRecException 
	 * @throws zbtree.IndexSearchException 
	 * @throws zbtree.IteratorException 
	 * @throws zbtree.LeafDeleteException 
	 * @throws zbtree.InsertException 
	 * @throws DeleteDescException 
	 * @throws IndexInsertDescException 
	 * @throws LeafInsertDescException 
	 * @throws DescriptorNotMatchException 
	 * @throws zbtree.GetFileEntryException 
	 * @throws zbtree.AddFileEntryException 
	 */
	public NID insertNode(byte[] recPtr)
			throws InvalidSlotNumberException,
			InvalidTupleSizeException,
			SpaceNotAvailableException,
			HFException,
			HFBufMgrException,
			HFDiskMgrException,
			IOException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, GetFileEntryException, AddFileEntryException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, zbtree.KeyTooLongException, zbtree.KeyNotMatchException, zbtree.LeafInsertRecException, zbtree.IndexInsertRecException, zbtree.ConstructPageException, zbtree.UnpinPageException, zbtree.PinPageException, zbtree.NodeNotMatchException, zbtree.ConvertException, zbtree.DeleteRecException, zbtree.IndexSearchException, zbtree.IteratorException, zbtree.LeafDeleteException, zbtree.InsertException, DescriptorNotMatchException, LeafInsertDescException, IndexInsertDescException, DeleteDescException, zbtree.GetFileEntryException, zbtree.AddFileEntryException {
		int dpinfoLen = 0;
		int recLen = recPtr.length;
		boolean found;
		NID currentDataPageNid = new NID();
		Page pageinbuffer = new Page();
		NHFPage currentDirPage = new NHFPage();
		NHFPage currentDataPage = new NHFPage();

		NHFPage nextDirPage = new NHFPage();
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId nextDirPageId = new PageId();  // OK

		pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

		found = false;
		Node anode;
		DataPageInfo dpinfo = new DataPageInfo();
		while (found == false) { //Start While01
			// look for suitable dpinfo-struct
			for (currentDataPageNid = currentDirPage.firstNode();
					currentDataPageNid != null;
					currentDataPageNid = currentDirPage.nextNode(currentDataPageNid)) {
				anode = currentDirPage.getNode(currentDataPageNid);
				dpinfo = new DataPageInfo(anode);
				// need check the record length == DataPageInfo'slength
				if(recLen <= dpinfo.availspace) {
					found = true;
					break;
				}
			}

			// two cases:
			// (1) found == true:
			//     currentDirPage has a datapagerecord which can accomodate
			//     the record which we have to insert
			// (2) found == false:
			//     there is no datapagerecord on the current directory page
			//     whose corresponding datapage has enough space free
			//     several subcases: see below
			if(!found)
			{ //Start IF01
				// case (2)

				//System.out.println("no datapagerecord on the current directory is OK");
				//System.out.println("dirpage availspace "+currentDirPage.available_space());

				// on the current directory page is no datapagerecord which has
				// enough free space
				//
				// two cases:
				//
				// - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
				//         if there is enough space on the current directory page
				//         to accomodate a new datapagerecord (type DataPageInfo),
				//         then insert a new DataPageInfo on the current directory
				//         page
				// - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
				//         look at the next directory page, if necessary, create it.

				if(currentDirPage.available_space() >= dpinfo.size) {
					//Start IF02
					// case (2.1) : add a new data page record into the
					//              current directory page
					currentDataPage = _newDatapage(dpinfo);
					// currentDataPage is pinned! and dpinfo->pageId is also locked
					// in the exclusive mode
					// didn't check if currentDataPage==NULL, auto exception
					// currentDataPage is pinned: insert its record
					// calling a HFPage function

					anode = dpinfo.convertToNode();
					byte [] tmpData = anode.getTupleByteArray();
					currentDataPageNid = currentDirPage.insertNode(tmpData);

					NID tmpnid = currentDirPage.firstNode();


					// need catch error here!
					if(currentDataPageNid == null)
						throw new HFException(null, "no space to insert rec.");

					// end the loop, because a new datapage with its record
					// in the current directorypage was created and inserted into
					// the heapfile; the new datapage has enough space for the
					// record which the user wants to insert

					found = true;

				} else {  //Start else 02
					// case (2.2)
					nextDirPageId = currentDirPage.getNextPage();
					// two sub-cases:
					//
					// (2.2.1) nextDirPageId != INVALID_PAGE:
					//         get the next directory page from the buffer manager
					//         and do another look
					// (2.2.2) nextDirPageId == INVALID_PAGE:
					//         append a new directory page at the end of the current
					//         page and then do another loop

					if (nextDirPageId.pid != INVALID_PAGE) { //Start IF03
						// case (2.2.1): there is another directory page:
						unpinPage(currentDirPageId, false);
						currentDirPageId.pid = nextDirPageId.pid;
						pinPage(currentDirPageId,
								currentDirPage, false);

						// now go back to the beginning of the outer while-loop and
						// search on the current directory page for a suitable datapage
					} else {  //Start Else03
						// case (2.2): append a new directory page after currentDirPage
						//             since it is the last directory page
						nextDirPageId = newPage(pageinbuffer, 1);
						// need check error!
						if(nextDirPageId == null)
							throw new HFException(null, "can't new pae");

						// initialize new directory page
						nextDirPage.init(nextDirPageId, pageinbuffer);
						PageId temppid = new PageId(INVALID_PAGE);
						nextDirPage.setNextPage(temppid);
						nextDirPage.setPrevPage(currentDirPageId);

						// update current directory page and unpin it
						// currentDirPage is already locked in the Exclusive mode
						currentDirPage.setNextPage(nextDirPageId);
						unpinPage(currentDirPageId, true/*dirty*/);

						currentDirPageId.pid = nextDirPageId.pid;
						currentDirPage = new NHFPage(nextDirPage);

						// remark that MINIBASE_BM->newPage already
						// pinned the new directory page!
						// Now back to the beginning of the while-loop, using the
						// newly created directory page.

					} //End of else03
				} // End of else02
				// ASSERTIONS:
				// - if found == true: search will end and see assertions below
				// - if found == false: currentDirPage, currentDirPageId
				//   valid and pinned

			} else { //Start else01
				// found == true:
				// we have found a datapage with enough space,
				// but we have not yet pinned the datapage:

				// ASSERTIONS:
				// - dpinfo valid

				// System.out.println("find the dirpagerecord on current page");

				pinPage(dpinfo.pageId, currentDataPage, false);
				//currentDataPage.openHFpage(pageinbuffer);
			}//End else01
		} //end of While01

		// ASSERTIONS:
		// - currentDirPageId, currentDirPage valid and pinned
		// - dpinfo.pageId, currentDataPageNid valid
		// - currentDataPage is pinned!

		if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
			throw new HFException(null, "invalid PageId");

		if (!(currentDataPage.available_space() >= recLen))
			throw new SpaceNotAvailableException(null, "no available space");

		if (currentDataPage == null)
			throw new HFException(null, "can't find Data page");


		NID nid;
		nid = currentDataPage.insertNode(recPtr);

		dpinfo.recct++;
		dpinfo.availspace = currentDataPage.available_space();


		unpinPage(dpinfo.pageId, true /* = DIRTY */);

		// DataPage is now released
		anode = currentDirPage.returnNode(currentDataPageNid);
		DataPageInfo dpinfo_ondirpage = new DataPageInfo(anode);


		dpinfo_ondirpage.availspace = dpinfo.availspace;
		dpinfo_ondirpage.recct = dpinfo.recct;
		dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
		dpinfo_ondirpage.flushToNode();
		unpinPage(currentDirPageId, true /* = DIRTY */);
		Node nn= new Node();
		nn.nodeInit(recPtr,0);

		if(SystemDefs.JavabaseDB.btNodeLabel!=null) { 
			SystemDefs.JavabaseDB.btNodeLabel = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeNodeIndex", AttrType.attrString, REC_LEN1, 1/*delete*/);
			SystemDefs.JavabaseDB.btNodeLabel.insert(new StringKey(nn.getLabel()),nid);
			SystemDefs.JavabaseDB.btNodeLabel.close();
		}
		if(SystemDefs.JavabaseDB.ztNodeDesc!=null) { 
			SystemDefs.JavabaseDB.ztNodeDesc=new ZBTreeFile(SystemDefs.JavabaseDBName+"_ZTreeNodeIndex", AttrType.attrDesc, 180, 1/*delete*/);
			SystemDefs.JavabaseDB.ztNodeDesc.insert(new DescriptorKey(nn.getDesc()),nid);
			SystemDefs.JavabaseDB.ztNodeDesc.close();
		}
		
		// TODO
		//SystemDefs.JavabaseDB.btNodeLabel.insert(new StringKey(nn.getLabel()),nid);

		return nid;
	}


	/** Updates the specified record in the heapfile.
	 * @param rid: the record which needs update
	 * @param newtuple: the new content of the record
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidUpdateException invalid update on record
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 * @return ture:update success   false: can't find the record
	 */
	public boolean updateNode(NID nid, Node newnode)
			throws InvalidSlotNumberException,
			InvalidUpdateException,
			InvalidTupleSizeException,
			HFException,
			HFDiskMgrException,
			HFBufMgrException,
			Exception {
		boolean status;
		NHFPage dirPage = new NHFPage();
		PageId currentDirPageId = new PageId();
		NHFPage dataPage = new NHFPage();
		PageId currentDataPageId = new PageId();
		NID currentDataPageNid = new NID();

		status = _findDataPage(nid,
				currentDirPageId, dirPage,
				currentDataPageId, dataPage,
				currentDataPageNid);

		if(!status) return status;	// record not found
		Node anode = new Node();
		anode = dataPage.returnNode(nid);

		// Assume update a record with a record whose length is equal to
		// the original record
		if(newnode.getLength() != anode.getLength()) {
			unpinPage(currentDataPageId, false /*undirty*/);
			unpinPage(currentDirPageId, false /*undirty*/);

			throw new InvalidUpdateException(null, "invalid record update");
		}

		// new copy of this record fits in old space;
		anode.nodeCopy(newnode);
		unpinPage(currentDataPageId, true /* = DIRTY */);
		unpinPage(currentDirPageId, false /*undirty*/);

		return true;
	}

	/** Read record from file, returning pointer and length.
	 * @param rid Record ID
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception SpaceNotAvailableException no space left
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 *
	 * @return a Tuple. if Tuple==null, no more tuple
	 */
	public  Node getNode(NID nid)
			throws InvalidSlotNumberException,
			InvalidTupleSizeException,
			HFException,
			HFDiskMgrException,
			HFBufMgrException,
			Exception
	{
		boolean status;
		NHFPage dirPage = new NHFPage();
		PageId currentDirPageId = new PageId();
		NHFPage dataPage = new NHFPage();
		PageId currentDataPageId = new PageId();
		NID currentDataPageNid = new NID();

		status = _findDataPage(nid,
				currentDirPageId, dirPage,
				currentDataPageId, dataPage,
				currentDataPageNid);

		if(!status) return null; // record not found
		Node anode = new Node();
		anode = dataPage.getNode(nid);

		/*
		 * getNode has copied the contents of nid into recPtr and fixed up
		 * recLen also.  We simply have to unpin dirpage and datapage which
		 * were originally pinned by _findDataPage.
		 */
		unpinPage(currentDataPageId,false /*undirty*/);
		unpinPage(currentDirPageId,false /*undirty*/);

		return  anode;  //(true?)OK, but the caller need check if atuple==NULL
	}

	/** Initiate a sequential scan.
	 * @exception InvalidTupleSizeException Invalid tuple size
	 * @exception IOException I/O errors
	 *
	 */
	//TODO: NScan
	public NScan openScan()
			throws InvalidTupleSizeException,
			IOException
	{
		NScan newscan = new NScan(this);
		return newscan;
	}

	public void deleteFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException,
	HFBufMgrException, HFDiskMgrException, IOException {
		if(_file_deleted )
			throw new FileAlreadyDeletedException(null, "file alread deleted");

		// Mark the deleted flag (even if it doesn't get all the way done).
		_file_deleted = true;

		// Deallocate all data pages
		PageId currentDirPageId = new PageId();
		currentDirPageId.pid = _firstDirPageId.pid;
		PageId nextDirPageId = new PageId();
		nextDirPageId.pid = 0;
		Page pageinbuffer = new Page();
		NHFPage currentDirPage =  new NHFPage();
		Node anode;

		pinPage(currentDirPageId, currentDirPage, false);
		//currentDirPage.openHFpage(pageinbuffer);

		NID nid = new NID();
		while(currentDirPageId.pid != INVALID_PAGE)
		{
			for(nid = currentDirPage.firstNode();
					nid != null;
					nid = currentDirPage.nextNode(nid))
			{
				anode = currentDirPage.getNode(nid);
				DataPageInfo dpinfo = new DataPageInfo( anode);
				//int dpinfoLen = arecord.length;

				freePage(dpinfo.pageId);

			}
			// ASSERTIONS:
			// - we have freePage()'d all data pages referenced by
			// the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			freePage(currentDirPageId);

			currentDirPageId.pid = nextDirPageId.pid;
			if (nextDirPageId.pid != INVALID_PAGE)
			{
				pinPage(currentDirPageId, currentDirPage, false);
				//currentDirPage.openHFpage(pageinbuffer);
			}
		}

		delete_file_entry( _fileName );
	}

	/**
	 * short cut to access the pinPage function in bufmgr package.
	 * @see bufmgr.pinPage
	 */
	private void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"Heapfile.java: pinPage() failed");
		}

	} // end of pinPage

	/**
	 * short cut to access the unpinPage function in bufmgr package.
	 * @see bufmgr.unpinPage
	 */
	private void unpinPage(PageId pageno, boolean dirty)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
		}

	} // end of unpinPage

	private void freePage(PageId pageno)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"Heapfile.java: freePage() failed");
		}

	} // end of freePage

	private PageId newPage(Page page, int num)
			throws HFBufMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseBM.newPage(page,num);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
		}

		return tmpId;

	} // end of newPage

	private PageId get_file_entry(String filename)
			throws HFDiskMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
		}

		return tmpId;

	} // end of get_file_entry

	private void add_file_entry(String filename, PageId pageno)
			throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
		}

	} // end of add_file_entry

	private void delete_file_entry(String filename)
			throws HFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		}
		catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
		}

	} // end of delete_file_entry

	/** Delete record from file with given rid.
	 *
	 * @exception InvalidSlotNumberException invalid slot number
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception HFException heapfile exception
	 * @exception HFBufMgrException exception thrown from bufmgr layer
	 * @exception HFDiskMgrException exception thrown from diskmgr layer
	 * @exception Exception other exception
	 *
	 * @return true record deleted  false:record not found
	 */
	public boolean deleteNode(NID nid)  
			throws InvalidSlotNumberException, 
			InvalidTupleSizeException, 
			HFException, 
			HFBufMgrException,
			HFDiskMgrException,
			Exception

	{
		if(SystemDefs.JavabaseDB.btNodeLabel!=null) { 
			SystemDefs.JavabaseDB.btNodeLabel = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeNodeIndex", AttrType.attrString, REC_LEN1, 1/*delete*/);
			SystemDefs.JavabaseDB.btNodeLabel.Delete(new StringKey(SystemDefs.JavabaseDB.nhfile.getNode(nid).getLabel()),nid);
			SystemDefs.JavabaseDB.btNodeLabel.close();
		}

        if(SystemDefs.JavabaseDB.ztNodeDesc!=null) { 
            SystemDefs.JavabaseDB.ztNodeDesc=new ZBTreeFile(SystemDefs.JavabaseDBName+"_ZTreeNodeIndex", AttrType.attrDesc, 180, 1/*delete*/);
            SystemDefs.JavabaseDB.ztNodeDesc.Delete(new DescriptorKey(SystemDefs.JavabaseDB.nhfile.getNode(nid).getDesc()),nid);
            SystemDefs.JavabaseDB.ztNodeDesc.close();
        }
		
		boolean status;
		NHFPage currentDirPage = new NHFPage();
		PageId currentDirPageId = new PageId();
		NHFPage currentDataPage = new NHFPage();
		PageId currentDataPageId = new PageId();
		NID currentDataPageRid = new NID();

		status = _findDataPage(nid,
				currentDirPageId, currentDirPage, 
				currentDataPageId, currentDataPage,
				currentDataPageRid);

		if(status != true) return status;	// record not found

		// ASSERTIONS:
			// - currentDirPage, currentDirPageId valid and pinned
		// - currentDataPage, currentDataPageid valid and pinned

		// get datapageinfo from the current directory page:
		Node anode;	

		anode = currentDirPage.returnNode(currentDataPageRid);
		DataPageInfo pdpinfo = new DataPageInfo(anode);

		// delete the record on the datapage
		currentDataPage.deleteNode(nid);

		pdpinfo.recct--;
		pdpinfo.flushToNode();	//Write to the buffer pool
		if (pdpinfo.recct >= 1) 
		{
			// more records remain on datapage so it still hangs around.  
			// we just need to modify its directory entry

			pdpinfo.availspace = currentDataPage.available_space();
			pdpinfo.flushToNode();
			unpinPage(currentDataPageId, true /* = DIRTY*/);

			unpinPage(currentDirPageId, true /* = DIRTY */);


		}
		else
		{
			// the record is already deleted:
			// we're removing the last record on datapage so free datapage
			// also, free the directory page if 
			//   a) it's not the first directory page, and 
			//   b) we've removed the last DataPageInfo record on it.

			// delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
			unpinPage(currentDataPageId, false /*undirty*/);

			freePage(currentDataPageId);

			// delete corresponding DataPageInfo-entry on the directory page:
			// currentDataPageRid points to datapage (from for loop above)

			currentDirPage.deleteNode(currentDataPageRid);


			// ASSERTIONS:
			// - currentDataPage, currentDataPageId invalid
			// - empty datapage unpinned and deleted

			// now check whether the directory page is empty:

			currentDataPageRid = currentDirPage.firstNode();

			// st == OK: we still found a datapageinfo record on this directory page
			PageId pageId;
			pageId = currentDirPage.getPrevPage();
			if((currentDataPageRid == null)&&(pageId.pid != INVALID_PAGE))
			{
				// the directory-page is not the first directory page and it is empty:
				// delete it

				// point previous page around deleted page:

				NHFPage prevDirPage = new NHFPage();
				pinPage(pageId, prevDirPage, false);

				pageId = currentDirPage.getNextPage();
				prevDirPage.setNextPage(pageId);
				pageId = currentDirPage.getPrevPage();
				unpinPage(pageId, true /* = DIRTY */);


				// set prevPage-pointer of next Page
				pageId = currentDirPage.getNextPage();
				if(pageId.pid != INVALID_PAGE)
				{
					NHFPage nextDirPage = new NHFPage();
					pageId = currentDirPage.getNextPage();
					pinPage(pageId, nextDirPage, false);

					//nextDirPage.openHFpage(apage);

					pageId = currentDirPage.getPrevPage();
					nextDirPage.setPrevPage(pageId);
					pageId = currentDirPage.getNextPage();
					unpinPage(pageId, true /* = DIRTY */);

				}

				// delete empty directory page: (automatically unpinned?)
				unpinPage(currentDirPageId, false/*undirty*/);
				freePage(currentDirPageId);


			}
			else
			{
				// either (the directory page has at least one more datapagerecord
				// entry) or (it is the first directory page):
				// in both cases we do not delete it, but we have to unpin it:

				unpinPage(currentDirPageId, true /* == DIRTY */);


			}
		}
		
		return true;
	}

}
