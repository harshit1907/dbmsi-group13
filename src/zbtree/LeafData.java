package zbtree;

import global.NID;

/**
 * Created by prakhar on 3/8/17.
 */
public class LeafData extends DataClass {
    private NID myNid;

    public String toString() {
        return "[" + (Integer.toString(myNid.pageNo.pid)) + " "
                + Integer.toString(myNid.slotNo) + " ]";
    }

    public LeafData(NID nid) {
        this.myNid = new NID(nid.pageNo, nid.slotNo);
    }

    public NID getData() {
        return this.myNid;
    }

    public void setData(NID nid) {
        this.myNid = new NID(nid.pageNo, nid.slotNo);
    }
}
