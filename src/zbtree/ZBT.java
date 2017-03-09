package zbtree;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by prakhar on 3/8/17.
 */
public class ZBT implements GlobalConst {
    /**
     *
     * @param key1
     * @param key2
     * @return
     * @throws DescriptorNotMatchException
     */
    public final static int keyCompare(KeyClass key1, KeyClass key2) throws DescriptorNotMatchException {
        if (key1 instanceof DescriptorKey && key2 instanceof DescriptorKey) {
            return ((DescriptorKey) key1).getKey().compareTo(((DescriptorKey) key2).getKey());
        } else {
            throw new DescriptorNotMatchException(null, "Descriptor Key Types do not match");
        }
    }

    /**
     *
     * @param key
     * @return
     * @throws DescriptorNotMatchException
     * @throws IOException
     */
    protected final static int getKeyLength(KeyClass key) throws DescriptorNotMatchException, IOException {
        if (key instanceof DescriptorKey) {
            return 20; // We know out key is of size 160/8 (32 x 5)
        } else {
            throw new DescriptorNotMatchException(null, "Descriptor key Types do not match");
        }
    }

    /**
     *
     * @param pageType
     * @return
     * @throws DescriptorNotMatchException
     */
    protected final static int getDataLength(short pageType) throws DescriptorNotMatchException {
        if ( pageType == NodeType.LEAF) {
            return 8;
        } else if ( pageType == NodeType.INDEX) {
            return 4;
        } else {
            throw new DescriptorNotMatchException(null, "Descriptor Key Types do not match");
        }
    }

    /**
     *
     * @param key
     * @param pageType
     * @return
     * @throws DescriptorNotMatchException
     * @throws NodeNotMatchException
     * @throws IOException
     */
    protected final static int getKeyDataLength(KeyClass key, short pageType)
            throws DescriptorNotMatchException, NodeNotMatchException, IOException {
        return getKeyLength(key) + getDataLength(pageType);
    }

    /**
     *
     * @param from
     * @param offset
     * @param length
     * @param keyType
     * @param nodeType
     * @return
     * @throws DescriptorNotMatchException
     * @throws NodeNotMatchException
     * @throws ConvertException
     */
    // TODO: This is doubtful
    protected final static DescriptorDataEntry getEntryFromBytes(byte[] from, int offset, int length,
                                                                 int keyType, short nodeType)
            throws DescriptorNotMatchException, NodeNotMatchException, ConvertException {
        KeyClass key;
        DataClass data;
        int n;
        try {
            if (nodeType == NodeType.INDEX) {
                n = 4;
                data = new IndexData(Convert.getIntValue(offset+length-4, from));
            }
            else if (nodeType == NodeType.LEAF) {
                n = 8;
                NID nid = new NID();
                nid.slotNo = Convert.getIntValue(offset+length-8, from);
                nid.pageNo = new PageId();
                nid.pageNo.pid = Convert.getIntValue(offset+length-4, from);
                data = new LeafData(nid);
            }
            else throw new NodeNotMatchException(null, "node types do not match");

            if (keyType == AttrType.attrDesc) {
                key = new DescriptorKey(Convert.getDescValue(offset, from));
            } else
                throw new DescriptorNotMatchException(null, "key types do not match");

            return new DescriptorDataEntry(key, data);
        } catch ( IOException e) {
            throw new ConvertException(e, "conversion failed");
        }
    }

    public static void printPage(PageId pageno, int keyType)
            throws IOException, IteratorException, ConstructPageException,
            HashEntryNotFoundException, ReplacerException,
            PageUnpinnedException, InvalidFrameNumberException {
        ZBTSortedPage sortedPage = new ZBTSortedPage(pageno, keyType);
        int i;
        i = 0;
        if (sortedPage.getType() == NodeType.INDEX) {
            ZBTIndexPage indexPage = new ZBTIndexPage((Page) sortedPage, keyType);
            System.out.println("");
            System.out.println("**************To Print an Index Page ********");
            System.out.println("Current Page ID: " + indexPage.getCurPage().pid);
            System.out.println("Left Link      : " + indexPage.getLeftLink().pid);

            NID nid = new NID();

            for (DescriptorDataEntry entry = indexPage.getFirst(nid); entry != null;
                 entry = indexPage.getNext(nid)) {
                if (keyType == AttrType.attrDesc) {
                    System.out.println(i + " (key, pageId):   (" +
                            (DescriptorKey) entry.key + ",  " + (IndexData) entry.data + " )");
                }
                i++;
            }
            System.out.println("************** END ********");
            System.out.println("");
        } else if (sortedPage.getType() == NodeType.LEAF) {
            ZBTLeafPage leafPage = new ZBTLeafPage((Page) sortedPage, keyType);
            System.out.println("");
            System.out.println("**************To Print an Leaf Page ********");
            System.out.println("Current Page ID: " + leafPage.getCurPage().pid);
            System.out.println("Left Link      : " + leafPage.getPrevPage().pid);
            System.out.println("Right Link     : " + leafPage.getNextPage().pid);

            RID rid = new RID();

            for (DescriptorDataEntry entry = leafPage.getFirst(rid); entry != null;
                 entry = leafPage.getNext(rid)) {
                if (keyType == AttrType.attrInteger)
                    System.out.println(i + " (key, [pageNo, slotNo]):   (" +
                            (DescriptorKey) entry.key + ",  " + (LeafData) entry.data + " )");
                i++;
            }
            System.out.println("************** END ********");
            System.out.println("");
        } else {
            System.out.println("Sorry!!! This page is neither Index nor Leaf page.");
        }
        SystemDefs.JavabaseBM.unpinPage(pageno, true/*dirty*/);
    }
}
