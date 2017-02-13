package global;

public class Descriptor {
	int value[] = new int[5];
	
	void set(int value0, int value1, int value2, int value3, int value4) {
		value[0] = value0;
		value[1] = value1;
		value[2] = value2;
		value[3] = value3;
		value[4] = value4;
	}
	
	int get(int idx) {
		return value[idx];
	}
	
	double equal(Descriptor desc) {
		return 0;
	}
	
	double distance(Descriptor desc) {
		return 0;
	}
}
