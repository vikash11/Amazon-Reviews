package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BrandCategoryPair implements WritableComparable {

	private Text brand;
	private Text category;

	public BrandCategoryPair(Text brand, Text category) {
		set(brand, category);
	}

	public BrandCategoryPair() {
		set(new Text(), new Text());
	}

	public BrandCategoryPair(String brand, String category) {
		set(new Text(brand), new Text(category));
	}

	public Text getBrand() {
		return brand;
	}

	public Text getCategory() {
		return category;
	}

	public void set(Text brand, Text category) {
		this.brand = brand;
		this.category = category;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		brand.readFields(in);
		category.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		brand.write(out);
		category.write(out);
	}

	@Override
	public String toString() {
		Object[] slist = new Object[2];
		slist[0] = this.brand;
		slist[1] = this.category;
		return StringUtils.join(slist, ',');
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BrandCategoryPair) {
			BrandCategoryPair tp = (BrandCategoryPair) o;
			return brand.equals(tp.brand) && category.equals(tp.category);
		}
		return false;
	}

	@Override
	public int compareTo(Object o) {
		if (o instanceof BrandCategoryPair) {
			BrandCategoryPair tp = (BrandCategoryPair) o;
			int cmp = brand.compareTo(tp.brand);
			if (cmp != 0) {
				return cmp;
			}
			return category.compareTo(tp.category);
		}
		return 0;
	}
}
