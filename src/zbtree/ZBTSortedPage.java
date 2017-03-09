package zbtree;

import diskmgr.Page;
import global.NID;
import global.PageId;
import global.SystemDefs;
import nodeheap.InvalidSlotNumberException;
import nodeheap.NHFPage;

import java.io.IOException;

/**
 * Created by prakhar on 3/8/17.
 */
public class ZBTSortedPage extends NHFPage {
    int keyType; // Initialized in ZBT file;

    public ZBTSortedPage(PageId pageno, int keyType) throws ConstructPageException {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
            this.keyType=keyType;
        } catch (Exception e) {
            throw new ConstructPageException(e, "construct sorted page failed");
        }
    }

    public ZBTSortedPage(Page page, int keyType) {
        super(page);
        this.keyType=keyType;
    }

    public ZBTSortedPage(int keyType) throws ConstructPageException {
        super();
        try {
            Page apage=new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage,1);
            if (pageId==null) {
                throw new ConstructPageException(null, "construct new page failed");
            }
            this.init(pageId, apage);
            this.keyType=keyType;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ConstructPageException(e, "construct sorted page failed");
        }
    }

    /**
     *
     * @param entry
     * @return
     * @throws InsertDescException
     */
    protected NID insertRecord(DescriptorDataEntry entry) throws InsertDescException {
        int i;
        short nType;
        NID nid;
        byte[] record;

        try {
            record = ZBT.getBytesFromEntry(entry);
            nid = super.insertNode(record);
            if (nid == null) {
                return null;
            }

            if (entry.data instanceof LeafData) {
                nType = NodeType.LEAF;
            } else {  //  entry.data instanceof IndexData
                nType = NodeType.INDEX;
            }

            for (i = getSlotCnt()-1; i > 0; i--) {
                KeyClass key_i, key_iplus1;

                key_i = ZBT.getEntryFromBytes(getpage(), getSlotOffset(i),
                        getSlotLength(i), keyType, nType).key;

                key_iplus1 = ZBT.getEntryFromBytes(getpage(), getSlotOffset(i-1),
                        getSlotLength(i-1), keyType, nType).key;

                if (ZBT.keyCompare(key_i, key_iplus1) < 0)
                {
                    // switch slots:
                    int ln, off;
                    ln = getSlotLength(i);
                    off = getSlotOffset(i);
                    setSlot(i, getSlotLength(i-1), getSlotOffset(i-1));
                    setSlot(i-1, ln, off);
                } else {
                    // end insertion sort
                    break;
                }
            }

            // ASSERTIONS:
            // - record keys increase with increasing slot number
            // (starting at slot 0)
            // - slot directory compacted

            nid.slotNo = i;
            return nid;
        } catch (Exception e ) {
            throw new InsertDescException(e, "insert descriptor failed");
        }
    }

    /**
     *
     * @param nid
     * @return
     * @throws DeleteDescException
     */
    public boolean deleteSortedRecord(NID nid) throws DeleteDescException {
        try {

            deleteNode(nid);
            compact_slot_dir();
            return true;
            // ASSERTIONS:
            // - slot directory is compacted
        } catch (Exception  e) {
            if (e instanceof InvalidSlotNumberException)
                return false;
            else
                throw new DeleteDescException(e, "delete record failed");
        }
    }

    /**
     *
     * @return
     * @throws IOException
     */
    protected int numberOfRecords() throws IOException {
        return getSlotCnt();
    }
}
