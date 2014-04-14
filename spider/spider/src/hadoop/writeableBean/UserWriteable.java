package hadoop.writeableBean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserWriteable implements WritableComparable<UserWriteable>{
	private String id;
	private String name;
	private String sign;
	private String detail;


	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(UserWriteable o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
