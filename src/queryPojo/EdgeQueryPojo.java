package queryPojo;

import global.NID;

/**
 * Key = 1 : Label
 * Key = 2 : Weight
 * Key = 3 : Source Label
 * Key = 4 : Dest Label
 * key=5 : nid
 */
public class EdgeQueryPojo {
    public EdgeQueryPojo() {
    }

    public void setEdgelabel(String edgelabel) {
        this.key = 1;
        this.edgelabel = edgelabel;
    }

    private String edgelabel;
    private String sourceLabel;
    private String destLabel;

    public NID getNd() {
        return nd;
    }

    public void setNd(NID nd) {
    	this.key = 5;
        this.nd = nd;
    }

    private NID nd;

    public void setWeight(int weight) {
        this.key = 2;
        this.weight = weight;
    }

    private int weight;
    private int key;

    public String getSourceLabel() {
        return sourceLabel;
    }

    public void setSourceLabel(String sourceLabel) {
        this.key = 3;
        this.sourceLabel = sourceLabel;
    }

    public String getDestLabel() {
        return destLabel;
    }

    public void setDestLabel(String destLabel) {
        this.key = 4;
        this.destLabel = destLabel;
    }

    public EdgeQueryPojo(String edgelabel) {
        this.key = 1;
        this.edgelabel = edgelabel;
    }

    public EdgeQueryPojo(int weight) {
        this.key = 2;
        this.weight = weight;
    }

    public int getKey() {
        return key;
    }
    public int getWeight() {
        return weight;
    }
    public String getEdgelabel() {
        return edgelabel;
    }
}
