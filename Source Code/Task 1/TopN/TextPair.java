package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable{
	
	   private Text first;
	   private Text second;
	 
	    public TextPair(Text first, Text second) {
	        set(first, second);
	    }
	 
	    public TextPair() {
	        set(new Text(), new Text());
	    }
	 
	    public TextPair(String first, String second) {
	        set(new Text(first), new Text(second));
	    }
	 
	    public Text getFirst() {
	        return first;
	    }
	 
	    public Text getSecond() {
	        return second;
	    }
	 
	    public void set(Text first, Text second) {
	        this.first = first;
	        this.second = second;
	    }
	 
	    @Override
	    public void readFields(DataInput in) throws IOException {
	    	first.readFields(in);
	    	second.readFields(in);
	    }
	 
	    @Override
	    public void write(DataOutput out) throws IOException {
	    	first.write(out);
	    	second.write(out);
	    }
	 
	    @Override
	    public String toString() {
	    	Object[] slist = new Object[3];
	    	slist[0] = this.first;
	    	slist[1] = this.second;
	    	slist[2] = "";
	    	return StringUtils.join(slist, ',');
	    }
	 
	    @Override
	    public int hashCode(){
	        return toString().hashCode();
	    }
	 
	    @Override
	    public boolean equals(Object o)
	    {
	        if(o instanceof TextPair)
	        {
	            TextPair tp = (TextPair) o;
	            return first.equals(tp.first) && second.equals(tp.second);
	        }
	        return false;
	    }

		@Override
		public int compareTo(Object o) {
			
			if(o instanceof TextPair){
				TextPair tp = (TextPair) o;
				int cmp = first.compareTo(tp.first);
				 
		        if (cmp != 0) {
		            return cmp;
		        }
			return second.compareTo(tp.second);
			}
			return 0;
		}
}