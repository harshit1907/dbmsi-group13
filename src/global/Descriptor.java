package global;

public class Descriptor {
    public int value[] = new int[5];

	public void set(int value0, int value1, int value2, int value3, int value4) {
		value[0] = value0;
		value[1] = value1;
		value[2] = value2;
		value[3] = value3;
		value[4] = value4;
	}

	public int get(int idx) {
		return value[idx];
	}

	public double equal(Descriptor desc) {
		if( this.value[0] == desc.value[0] &&
		this.value[1] == desc.value[1] &&
		this.value[2] == desc.value[2] &&
		this.value[3] == desc.value[3] &&
		this.value[4] == desc.value[4] )
			return  1;
		else 
			return 0;
	}
	
	public String getString() {
		String str="";
		for(int val:value)
		{
			str=str+val+"\t";
		}
		return str;
	}

	public double distance(Descriptor desc) {
		double d;
		d = Math.sqrt( Math.pow(this.value[0]-desc.value[0], 2) +
				Math.pow(this.value[1]-desc.value[1], 2) +
				Math.pow(this.value[2]-desc.value[2], 2) +
				Math.pow(this.value[3]-desc.value[3], 2) +
				Math.pow(this.value[4]-desc.value[4], 2) );
		return d;
	} 
}
