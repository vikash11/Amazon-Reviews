package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BrandStatistics implements WritableComparable {

	private DoubleWritable brandAvgRating;
	private DoubleWritable brandNumberOfReviews;
	private DoubleWritable brandScore;

	public BrandStatistics(DoubleWritable brandAvgRating, DoubleWritable brandNumberOfReviews,
			DoubleWritable brandScore) {
		set(brandAvgRating, brandNumberOfReviews, brandScore);
	}

	public BrandStatistics() {
		set(new DoubleWritable(), new DoubleWritable(), new DoubleWritable());
	}

	public BrandStatistics(double brandAvgRating, double brandNumberOfReviews, double brandScore) {
		set(new DoubleWritable(brandAvgRating), new DoubleWritable(brandNumberOfReviews),
				new DoubleWritable(brandScore));
	}

	public DoubleWritable getBrandAvgRating() {
		return brandAvgRating;
	}

	public DoubleWritable getBrandNumberOfReviews() {
		return brandNumberOfReviews;
	}

	public DoubleWritable getBrandScore() {
		return brandScore;
	}

	public void set(DoubleWritable brandAvgRating, DoubleWritable brandNumberOfReviews, DoubleWritable brandScore) {
		this.brandAvgRating = brandAvgRating;
		this.brandNumberOfReviews = brandNumberOfReviews;
		this.brandScore = brandScore;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		brandAvgRating.readFields(in);
		brandNumberOfReviews.readFields(in);
		brandScore.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		brandAvgRating.write(out);
		brandNumberOfReviews.write(out);
		brandScore.write(out);
	}

	@Override
	public String toString() {
		Object[] slist = new Object[4];
		slist[0] = "";
		slist[1] = this.brandAvgRating;
		slist[2] = this.brandNumberOfReviews;
		slist[3] = this.brandScore;
		return StringUtils.join(slist, ',');
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BrandStatistics) {
			BrandStatistics tp = (BrandStatistics) o;
			return brandAvgRating.equals(tp.brandAvgRating) && brandNumberOfReviews.equals(tp.brandNumberOfReviews)
					&& brandScore.equals(tp.brandScore);
		}
		return false;
	}

	@Override
	public int compareTo(Object o) {
		if (o instanceof BrandStatistics) {
			BrandStatistics tp = (BrandStatistics) o;
			int cmp = brandAvgRating.compareTo(tp.brandAvgRating);
			if (cmp != 0) {
				return cmp;
			}
			return brandNumberOfReviews.compareTo(tp.brandNumberOfReviews);
		}
		return 0;
	}
}
