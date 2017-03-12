package global;

import zbtree.DescriptorKey;

/**
 * Created by prakhar on 3/11/17.
 */
public class DescriptorRangePair {
    public DescriptorKey getStart() {
        return start;
    }

    public void setStart(DescriptorKey start) {
        this.start = start;
    }

    public DescriptorKey getEnd() {
        return end;
    }

    public void setEnd(DescriptorKey end) {
        this.end = end;
    }

    private DescriptorKey start;
    private DescriptorKey end;

    public DescriptorRangePair(DescriptorKey start, DescriptorKey end) {
        this.start = start;
        this.end = end;
    }
}
