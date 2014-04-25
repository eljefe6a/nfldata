import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class PassWritable implements Writable {
	public int passes;
	public int season;
	
	public PassWritable() {
		
	}
	
	public PassWritable(int passes, int season) {
		this.passes = passes;
		this.season = season;
	}
	
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(passes);
		out.writeInt(season);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		passes = in.readInt();
		season = in.readInt();
	}

	@Override
	public String toString() {
		return passes + "\t" + season;
	}
}
