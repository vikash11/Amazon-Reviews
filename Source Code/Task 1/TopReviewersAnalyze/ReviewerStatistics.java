package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ReviewerStatistics implements WritableComparable {

	private DoubleWritable reviewerAvgRating;
	private DoubleWritable reviewerNumberOfReviews;
	private DoubleWritable reviewerScore;

	public ReviewerStatistics(DoubleWritable reviewerAvgRating, DoubleWritable reviewerNumberOfReviews,
			DoubleWritable reviewerScore) {
		set(reviewerAvgRating, reviewerNumberOfReviews, reviewerScore);
	}

	public ReviewerStatistics() {
		set(new DoubleWritable(), new DoubleWritable(), new DoubleWritable());
	}

	public ReviewerStatistics(double reviewerAvgRating, double reviewerNumberOfReviews, double reviewerScore) {
		set(new DoubleWritable(reviewerAvgRating), new DoubleWritable(reviewerNumberOfReviews),
				new DoubleWritable(reviewerScore));
	}

	public DoubleWritable getBrandAvgRating() {
		return reviewerAvgRating;
	}

	public DoubleWritable getBrandNumberOfReviews() {
		return reviewerNumberOfReviews;
	}

	public DoubleWritable getBrandScore() {
		return reviewerScore;
	}

	public void set(DoubleWritable reviewerAvgRating, DoubleWritable brandNumberOfReviews, DoubleWritable brandScore) {
		this.reviewerAvgRating = reviewerAvgRating;
		this.reviewerNumberOfReviews = brandNumberOfReviews;
		this.reviewerScore = brandScore;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		reviewerAvgRating.readFields(in);
		reviewerNumberOfReviews.readFields(in);
		reviewerScore.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		reviewerAvgRating.write(out);
		reviewerNumberOfReviews.write(out);
		reviewerScore.write(out);
	}

	@Override
	public String toString() {
		Object[] slist = new Object[4];
		slist[0] = "";
		slist[1] = this.reviewerAvgRating;
		slist[2] = this.reviewerNumberOfReviews;
		slist[3] = this.reviewerScore;
		return StringUtils.join(slist, ',');
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ReviewerStatistics) {
			ReviewerStatistics tp = (ReviewerStatistics) o;
			return reviewerAvgRating.equals(tp.reviewerAvgRating) && reviewerNumberOfReviews.equals(tp.reviewerNumberOfReviews)
					&& reviewerScore.equals(tp.reviewerScore);
		}
		return false;
	}

	@Override
	public int compareTo(Object o) {
		if (o instanceof ReviewerStatistics) {
			ReviewerStatistics tp = (ReviewerStatistics) o;
			int cmp = reviewerAvgRating.compareTo(tp.reviewerAvgRating);
			if (cmp != 0) {
				return cmp;
			}
			return reviewerNumberOfReviews.compareTo(tp.reviewerNumberOfReviews);
		}
		return 0;
	}
}
