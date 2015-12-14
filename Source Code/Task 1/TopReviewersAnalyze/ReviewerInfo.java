package model;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ReviewerInfo implements WritableComparable{
	
	   private Text reviewerId;
	   private Text reviewerName;
	   private Text category;
	 
	    public ReviewerInfo(Text reviewerId, Text reviewerName, Text category) {
	        set(reviewerId, reviewerName, category);
	    }
	 
	    public ReviewerInfo() {
	        set(new Text(), new Text(), new Text());
	    }
	 
	    public ReviewerInfo(String reviewerId, String reviewerName, String category) {
	        set(new Text(reviewerId), new Text(reviewerName), new Text(category));
	    }
	 
	    public Text getReviewerId() {
	        return reviewerId;
	    }
	 
	    public Text getReviewerName() {
	        return reviewerName;
	    }
	 
	    public Text getCategory(){
	    	return category;
	    }
	    public void set(Text reviewerId, Text reviewerName, Text category) {
	        this.reviewerId = reviewerId;
	        this.reviewerName = reviewerName;
	        this.category = category;
	    }
	 
	    @Override
	    public void readFields(DataInput in) throws IOException {
	    	reviewerId.readFields(in);
	    	reviewerName.readFields(in);
	    	category.readFields(in);
	    }
	 
	    @Override
	    public void write(DataOutput out) throws IOException {
	    	reviewerId.write(out);
	    	reviewerName.write(out);
	    	category.write(out);
	    }
	 
	    @Override
	    public String toString() {
	    	Object[] slist = new Object[3];
	    	slist[0] = this.reviewerId;
	    	slist[1] = this.reviewerName;
	    	slist[2] = this.category;
	        return StringUtils.join(slist, ',');
	    }
	 
	    @Override
	    public int hashCode(){
	        return reviewerId.hashCode()*163 + reviewerName.hashCode() + category.hashCode();
	    }
	 
	    @Override
	    public boolean equals(Object o)
	    {
	        if(o instanceof ReviewerInfo)
	        {
	        	ReviewerInfo tp = (ReviewerInfo) o;
	            return reviewerId.equals(tp.reviewerId) && reviewerName.equals(tp.reviewerName)
	            		&& category.equals(tp.category);
	        }
	        return false;
	    }

		@Override
		public int compareTo(Object o) {
			
			if(o instanceof ReviewerInfo){
				ReviewerInfo tp = (ReviewerInfo) o;
				int cmp = reviewerId.compareTo(tp.reviewerId);
				 
		        if (cmp != 0) {
		            return cmp;
		        }
			return reviewerName.compareTo(tp.reviewerName);
			}
			return 0;
		}
}
