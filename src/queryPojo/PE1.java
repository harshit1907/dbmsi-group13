package queryPojo;

import global.Descriptor;
import global.NID;

public class PE1 {
	
	NID nd;
	String label;
	Descriptor desc;
	int key;
	public int getKey() {
		return key;
	}
	public void setKey(int key) {
		this.key = key;
	}
	public NID getNd() {
		return nd;
	}
	public void setNd(NID nd) {
		this.nd = nd;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public Descriptor getDesc() {
		return desc;
	}
	public void setDesc(Descriptor desc) {
		this.desc = desc;
	}
	

}
