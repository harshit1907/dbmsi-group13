package global;

/**
 * Created by prakhar on 2/18/17.
 */
public class NID extends RID {
    /**
     * Default Constructor
     */
    public NID() {}

    /**
     *  constructor of class
     */
    public NID(PageId pageno, int slotno) {
        pageNo = pageno;
        slotNo = slotno;
    }

    /**
     * make a copy of the given rid
     */
    public void copyNid(NID nid)
    {
        pageNo = nid.pageNo;
        slotNo = nid.slotNo;
    }

    /** Compares two NID object, i.e, this to the rid
     * @param NID NID object to be compared to
     * @return true is they are equal
     *         false if not.
     */
    public boolean equals(NID nid) {
        return (this.pageNo.pid == nid.pageNo.pid)
                && (this.slotNo == nid.slotNo);
    }
}
