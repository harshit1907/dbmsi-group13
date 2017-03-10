package zbtree;

import diskmgr.Page;
import global.NID;
import global.PageId;

import java.io.IOException;

/**
 * Created by prakhar on 3/9/17.
 */
public class ZBTLeafPage extends ZBTSortedPage {
    public ZBTLeafPage(PageId pageno, int keyType) throws IOException, ConstructPageException {
        super(pageno, keyType);
        setType(NodeType.LEAF);
    }

    public ZBTLeafPage(Page page, int keyType) throws IOException, ConstructPageException {
        super(page, keyType);
        setType(NodeType.LEAF);
    }

    public ZBTLeafPage(int keyType) throws ConstructPageException, IOException {
        super(keyType);
        setType(NodeType.LEAF);
    }

    public NID insertRecord(KeyClass key, NID dataNid) throws LeafInsertDescException {
        DescriptorDataEntry entry;
        try {
            entry = new DescriptorDataEntry(key,dataNid);
            return insertRecord(entry);
        }
        catch(Exception e) {
            throw new LeafInsertDescException(e, "insert record failed");
        }
    } // end of insertRecord

    public DescriptorDataEntry getFirst(NID nid) throws IteratorException {
        DescriptorDataEntry entry;
        try {
            nid.pageNo = getCurPage();
            nid.slotNo = 0; // begin with first slot

            if ( getSlotCnt() <= 0) {
                return null;
            }

            entry = ZBT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0),
                    keyType, NodeType.LEAF);

            return entry;
        }
        catch (Exception e) {
            throw new IteratorException(e, "Get first entry failed");
        }
    } // end of getFirst

    public DescriptorDataEntry getNext (NID nid) throws  IteratorException {
        DescriptorDataEntry entry;
        int i;
        try{
            nid.slotNo++; //must before any return;
            i = nid.slotNo;

            if (nid.slotNo >= getSlotCnt()) {
                return null;
            }

            entry = ZBT.getEntryFromBytes(getpage(),getSlotOffset(i), getSlotLength(i),
                    keyType, NodeType.LEAF);

            return entry;
        }
        catch (Exception e) {
            throw new IteratorException(e,"Get next entry failed");
        }
    }

    public DescriptorDataEntry getCurrent (NID nid) throws  IteratorException {
        nid.slotNo--;
        return getNext(nid);
    }

    public boolean delEntry (DescriptorDataEntry dEntry) throws LeafDeleteException {
        DescriptorDataEntry entry;
        NID nid = new NID();

        try {
            for(entry = getFirst(nid); entry!=null; entry=getNext(nid)) {
                if ( entry.equals(dEntry) ) {
                    if (!super.deleteSortedRecord(nid))
                        throw new LeafDeleteException(null, "Delete record failed");
                    return true;
                }

            }
            return false;
        }
        catch (Exception e) {
            throw new LeafDeleteException(e, "delete entry failed");
        }

    } // end of delEntry

    public boolean redistribute(ZBTLeafPage leafPage, ZBTIndexPage parentIndexPage,
                         int direction, KeyClass deletedKey) throws LeafRedistributeException {
        boolean st;
        // assertion: leafPage pinned
        try {
            if (direction ==-1) { // 'this' is the left sibling of leafPage
                if ( (getSlotLength(getSlotCnt()-1) + available_space()+ 8 /*  2*sizeof(slot) */) >
                        ((MAX_SPACE-DPFIXED)/2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                }
                else {
                    // move the last record to its sibling

                    // get the last record
                    DescriptorDataEntry lastEntry;
                    lastEntry = ZBT.getEntryFromBytes(getpage(),getSlotOffset(getSlotCnt()-1)
                            ,getSlotLength(getSlotCnt()-1), keyType, NodeType.LEAF);


                    //get its sibling's first record's key for adjusting parent pointer
                    NID dummyNid = new NID();
                    DescriptorDataEntry firstEntry;
                    firstEntry = leafPage.getFirst(dummyNid);

                    // insert it into its sibling
                    leafPage.insertRecord(lastEntry);

                    // delete the last record from the old page
                    NID delNid = new NID();
                    delNid.pageNo = getCurPage();
                    delNid.slotNo = getSlotCnt() - 1;
                    if (!deleteSortedRecord(delNid)) {
                        throw new LeafRedistributeException(null, "delete record failed");
                    }

                    // adjust the entry pointing to sibling in its parent
                    if (deletedKey != null) {
                        st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
                    } else {
                        st = parentIndexPage.adjustKey(lastEntry.key,
                                firstEntry.key);
                    }
                    if (!st) {
                        throw new LeafRedistributeException(null, "adjust key failed");
                    }
                    return true;
                }
            } else { // 'this' is the right sibling of pptr
                if ( (getSlotLength(0) + available_space()+ 8) > ((MAX_SPACE-DPFIXED)/2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // move the first record to its sibling

                    // get the first record
                    DescriptorDataEntry firstEntry;
                    firstEntry = ZBT.getEntryFromBytes(getpage(), getSlotOffset(0),
                            getSlotLength(0), keyType,
                            NodeType.LEAF);

                    // insert it into its sibling
                    NID dummyNid=new NID();
                    leafPage.insertRecord(firstEntry);


                    // delete the first record from the old page
                    NID delRid = new NID();
                    delRid.pageNo = getCurPage();
                    delRid.slotNo = 0;
                    if (!deleteSortedRecord(delRid)) {
                        throw new LeafRedistributeException(null, "delete record failed");
                    }

                    // get the current first record of the old page
                    // for adjusting parent pointer.
                    DescriptorDataEntry tmpEntry;
                    tmpEntry = getFirst(dummyNid);

                    // adjust the entry pointing to itself in its parent
                    st = parentIndexPage.adjustKey(tmpEntry.key, firstEntry.key);
                    if(!st) {
                        throw new LeafRedistributeException(null, "adjust key failed");
                    }
                    return true;
                }
            }
        }
        catch (Exception e) {
            throw new LeafRedistributeException(e, "redistribute failed");
        }
    } // end of redistribute
}
