
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class myMapWritable extends MapWritable {

	public myMapWritable() {
		// TODO Auto-generated constructor stub
		super();
	}

	public myMapWritable(MapWritable other) {
		super(other);
		// TODO Auto-generated constructor stub
	}
	
		
    
	public final String toString() {
		String str = new String();
		Set<Writable> keys = this.keySet();
		for(Writable eachKey : keys){
			IntWritable value = (IntWritable)this.get(eachKey);
			str = str + " " + eachKey + " = " + value + ",";
		}
		
        return str;
    }
	

		
	

}