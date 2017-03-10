package zbtree;

import global.GlobalConst;
import global.NID;

/**
 * Created by prakhar on 3/9/17.
 */
public class ZBTFileScan extends IndexFileScan implements GlobalConst {
    ZBTreeFile bfile;
    String treeFilename;
    ZBTLeafPage leafPage;
    NID curNid;
    boolean didfirst;
    boolean deletedcurrent;

    KeyClass endkey;
    int keyType;
    int maxKeysize;

    @Override
    public DescriptorDataEntry get_next() throws ScanIteratorException {
        return null;
    }

    @Override
    public void delete_current() throws ScanDeleteException {

    }

    @Override
    public int keysize() {
        return 0;
    }
}
