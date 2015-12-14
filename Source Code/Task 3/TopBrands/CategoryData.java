package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CategoryData implements WritableComparable {

	private DoubleWritable totalCount;
	private DoubleWritable totalRating;

	public CategoryData(DoubleWritable totalCount, DoubleWritable totalRating) {
		set(totalCount, totalRating);
	}

	public CategoryData() {
		set(new DoubleWritable(), new DoubleWritable());
	}

	public CategoryData(double totalCount, double totalRating) {
		set(new DoubleWritable(totalCount), new DoubleWritable(totalRating));
	}

	public DoubleWritable getTotalCount() {
		return totalCount;
	}

	public DoubleWritable getTotalRating() {
		return totalRating;
	}

	public void set(DoubleWritable totalCount, DoubleWritable totalRating) {
		this.totalCount = totalCount;
		this.totalRating = totalRating;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		totalCount.readFields(in);
		totalRating.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		totalCount.write(out);
		totalRating.write(out);
	}

	@Override
	public String toString() {
		Object[] slist = new Object[2];
		slist[0] = this.totalCount;
		slist[1] = this.totalRating;
		return StringUtils.join(slist, ',');
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof CategoryData) {
			CategoryData tp = (CategoryData) o;
			return totalCount.equals(tp.totalCount) && totalRating.equals(tp.totalRating);
		}
		return false;
	}

	@Override
	public int compareTo(Object o) {
		if (o instanceof CategoryData) {
			CategoryData tp = (CategoryData) o;
			int cmp = totalCount.compareTo(tp.totalCount);
			if (cmp != 0) {
				return cmp;
			}
			return totalRating.compareTo(tp.totalRating);
		}
		return 0;
	}
}
