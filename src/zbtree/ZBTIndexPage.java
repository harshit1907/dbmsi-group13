package zbtree;

import diskmgr.Page;
import global.NID;
import global.PageId;

import java.io.IOException;

/**
 * Created by prakhar on 3/9/17.
 */
public class ZBTIndexPage extends ZBTSortedPage {
    public ZBTIndexPage(PageId pageno, int keyType) throws IOException, ConstructPageException {
        super(pageno, keyType);
        setType(NodeType.INDEX);
    }

    public ZBTIndexPage(Page page, int keyType) throws IOException, ConstructPageException {
        super(page, keyType);
        setType(NodeType.INDEX);
    }

    /**
     *
     * @param keyType
     * @throws IOException
     * @throws ConstructPageException
     */
    public ZBTIndexPage(int keyType) throws IOException, ConstructPageException {
        super(keyType);
        setType(NodeType.INDEX);
    }

    /**
     *
     * @param key
     * @param pageNo
     * @return
     * @throws IndexInsertDescException
     */
    public NID insertKey(KeyClass key, PageId pageNo) throws IndexInsertDescException {
        NID nid;
        DescriptorDataEntry entry;
        try {
            entry = new DescriptorDataEntry(key, pageNo);
            nid = super.insertRecord(entry);
            return nid;
        } catch (Exception e) {
            throw new IndexInsertDescException(e, "Descriptor Insert failed");
        }
    }

    /**
     *
     * @param key
     * @return
     * @throws IndexFullDeleteException
     */
    public NID deleteKey(KeyClass key) throws IndexFullDeleteException {
        DescriptorDataEntry entry;
        NID nid = new NID();

        try {
            entry = getFirst(nid);

            if (entry == null) {
                //it is supposed there is at least a record
                throw new IndexFullDeleteException(null, "No records found");
            }
            if (ZBT.keyCompare(key, entry.key) < 0) {
                //it is supposed to be not smaller than first key
                throw new IndexFullDeleteException(null, "First key is bigger");
            }
            while (ZBT.keyCompare(key, entry.key) > 0) {
                entry = getNext(nid);
                if (entry == null) {
                    break;
                }
            }

            if (entry == null) {
                nid.slotNo--;
            } else if (ZBT.keyCompare(key, entry.key) != 0) {
                nid.slotNo--; // we want to delete the previous key
            }

            deleteSortedRecord(nid);
            return nid;
        } catch (Exception e) {
            throw new IndexFullDeleteException(e, "Full delelte failed");
        }
    }

    public PageId getPageNoByKey(KeyClass key) throws IndexSearchException {
        DescriptorDataEntry entry;
        int i;
        try {
            for (i = getSlotCnt()-1; i >= 0; i--) {
                entry = ZBT.getEntryFromBytes( getpage(),getSlotOffset(i),
                        getSlotLength(i), keyType, NodeType.INDEX);

                if (ZBT.keyCompare(key, entry.key) >= 0) {
                    return ((IndexData)entry.data).getData();
                }
            }
            return getPrevPage();
        } catch (Exception e) {
            throw new IndexSearchException(e, "Get entry failed");
        }
    }

    public DescriptorDataEntry getFirst(NID nid) throws IteratorException {
        DescriptorDataEntry entry;

        try {
            nid.pageNo = getCurPage();
            nid.slotNo = 0; // begin with first slot

            if (getSlotCnt() == 0) {
                return null;
            }

            entry = ZBT.getEntryFromBytes( getpage(),getSlotOffset(0),
                    getSlotLength(0),
                    keyType, NodeType.INDEX);

            return entry;
        }
        catch (Exception e) {
            throw new IteratorException(e, "Get first entry failed");
        }

    } // end of getFirst

    public DescriptorDataEntry getNext (NID nid) throws  IteratorException {
        DescriptorDataEntry entry;
        int i;
        try {
            nid.slotNo++; //must before any return;
            i = nid.slotNo;

            if (nid.slotNo >= getSlotCnt()) {
                return null;
            }

            entry = ZBT.getEntryFromBytes(getpage(),getSlotOffset(i),
                    getSlotLength(i),
                    keyType, NodeType.INDEX);

            return entry;
        }
        catch (Exception e) {
            throw new IteratorException(e, "Get next entry failed");
        }
    } // end of getNext

    protected PageId getLeftLink() throws IOException {
        return getPrevPage();
    }

    protected void setLeftLink(PageId left) throws IOException {
        setPrevPage(left);
    }

    public int getSibling(KeyClass key, PageId pageNo) throws IndexFullDeleteException {
        try {
            if (getSlotCnt() == 0) // there is no sibling
                return 0;

            int i;
            DescriptorDataEntry entry;
            for (i = getSlotCnt()-1; i >= 0; i--) {
                entry = ZBT.getEntryFromBytes(getpage(), getSlotOffset(i),
                        getSlotLength(i), keyType , NodeType.INDEX);
                if (ZBT.keyCompare(key, entry.key)>=0) {
                    if (i != 0) {
                        entry = ZBT.getEntryFromBytes(getpage(), getSlotOffset(i-1),
                                getSlotLength(i-1), keyType, NodeType.INDEX);
                        pageNo.pid=  ((IndexData)entry.data).getData().pid;
                        return -1; //left sibling
                    }
                    else {
                        pageNo.pid = getLeftLink().pid;
                        return -1; //left sibling
                    }
                }
            }
            entry = ZBT.getEntryFromBytes(getpage(), getSlotOffset(0),
                    getSlotLength(0), keyType, NodeType.INDEX);
            pageNo.pid = ((IndexData)entry.data).getData().pid;
            return 1;  //right sibling
        }
        catch (Exception e) {
            throw new IndexFullDeleteException(e, "Get sibling failed");
        }
    } // end of getSibling

    public boolean adjustKey(KeyClass newKey, KeyClass oldKey) throws IndexFullDeleteException {
        try {
            DescriptorDataEntry entry;
            entry =  findKeyData( oldKey );
            if (entry == null) return false;

            NID nid=deleteKey( entry.key );
            if (nid==null) throw new IndexFullDeleteException(null, "nid is null");

            nid = insertKey( newKey, ((IndexData)entry.data).getData());
            if (nid == null) {
                throw new IndexFullDeleteException(null, "Nid is null");
            }

            return true;
        }
        catch (Exception e) {
            throw new IndexFullDeleteException(e, "Adjust key failed");
        }
    } // end of adjustKey

    public DescriptorDataEntry findKeyData(KeyClass key) throws IndexSearchException {
        DescriptorDataEntry entry;
        try {

            for (int i = getSlotCnt()-1; i >= 0; i--) {
                entry = ZBT.getEntryFromBytes(getpage(),getSlotOffset(i),
                        getSlotLength(i), keyType, NodeType.INDEX);

                if (ZBT.keyCompare(key, entry.key) >= 0) {
                    return entry;
                }
            }
            return null;
        }
        catch ( Exception e) {
            throw  new IndexSearchException(e, "finger key data failed");
        }
    } // end of findKeyData

    public KeyClass findKey(KeyClass key) throws IndexSearchException {
        return findKeyData(key).key;
    }

    public boolean redistribute(ZBTIndexPage indexPage, ZBTIndexPage parentIndexPage,
                         int direction, KeyClass deletedKey) throws RedistributeException {
        // assertion: indexPage and parentIndexPage are  pinned
        try {
            boolean st;
            if (direction==-1) { // 'this' is the left sibling of indexPage
                if ((getSlotLength(getSlotCnt()-1) + available_space()) > ((MAX_SPACE-DPFIXED)/2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // get its sibling's first record's key
                    NID dummyRid = new NID();
                    DescriptorDataEntry firstEntry, lastEntry;
                    firstEntry = indexPage.getFirst(dummyRid);
                    // get the entry pointing to the right sibling
                    KeyClass splitKey = parentIndexPage.findKey(firstEntry.key);
                    // get the leftmost child pointer of the right sibling
                    PageId leftMostPageId = indexPage.getLeftLink();
                    // insert  <splitKey,leftMostPageId>  to its sibling
                    indexPage.insertKey( splitKey, leftMostPageId);

                    // get the last record of itself
                    lastEntry = ZBT.getEntryFromBytes(getpage(), getSlotOffset(getSlotCnt()-1),
                            getSlotLength(getSlotCnt()-1), keyType, NodeType.INDEX);

                    // set sibling's leftmostchild to be lastPageId
                    indexPage.setLeftLink(((IndexData)(lastEntry.data)).getData() );

                    // delete the last record from the old page
                    NID delRid = new NID();
                    delRid.pageNo = getCurPage();
                    delRid.slotNo = getSlotCnt()-1;

                    if (!deleteSortedRecord(delRid)) {
                        throw new RedistributeException(null, "Delete record failed");
                    }

                    // adjust the entry pointing to sibling in its parent
                    if (deletedKey != null) {
                        st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
                    } else {
                        st = parentIndexPage.adjustKey(lastEntry.key, splitKey);
                    }
                    if (!st) {
                        throw new RedistributeException(null, "adjust key failed");
                    }
                    return true;
                }
            } else { // 'this' is the right sibling of indexPage
                if ( (getSlotLength(0) + available_space()) > ((MAX_SPACE-DPFIXED)/2) ) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // get the first record
                    DescriptorDataEntry firstEntry;
                    firstEntry = ZBT.getEntryFromBytes( getpage(),
                            getSlotOffset(0),
                            getSlotLength(0), keyType, NodeType.INDEX);

                    // get its leftmost child pointer
                    PageId leftMostPageId = getLeftLink();

                    // get the entry in its parent pointing to itself
                    KeyClass splitKey;
                    splitKey = parentIndexPage.findKey(firstEntry.key);

                    // insert <split, leftMostPageId> to its left sibling

                    indexPage.insertKey(splitKey,leftMostPageId);


                    // set its new leftmostchild
                    setLeftLink(((IndexData)(firstEntry.data)).getData());

                    // delete the first record
                    NID delNid=new NID();
                    delNid.pageNo = getCurPage();
                    delNid.slotNo = 0;
                    if (!deleteSortedRecord(delNid)) {
                        throw new RedistributeException(null, "delete record failed");
                    }

                    // adjust the entry pointing to itself in its parent
                    if (!parentIndexPage.adjustKey(firstEntry.key, splitKey)) {
                        throw new RedistributeException(null, "adjust key failed");
                    }
                    return true;
                }
            } //else
        } //try
        catch (Exception e){
            throw new RedistributeException(e, "redistribute failed");
        }
    } // end of redistribute
}
