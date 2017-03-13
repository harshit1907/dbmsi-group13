package zbtree;

import global.PageId;

/**
 * Created by prakhar on 3/8/17.
 */
public class IndexData extends DataClass {
    private PageId pageId;

    public IndexData(PageId pageId) {
        this.pageId = new PageId(pageId.pid);
    }

    public IndexData(int pageId) {
        this.pageId = new PageId(pageId);
    }

    public String toString() {
        return Integer.toString(pageId.pid);
    }

    public PageId getData() {
        return this.pageId;
    }

    public void setData(PageId pageId) {
        this.pageId = pageId;
    }
}
