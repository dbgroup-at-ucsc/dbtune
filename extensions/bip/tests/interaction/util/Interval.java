package interaction.util;

public class Interval {
	public double low;
	public double high;
	
	public Interval() {
		low = 0;
		high = Double.POSITIVE_INFINITY;
	}
	
	public Interval(double low0, double high0) {
		low = low0;
		high = high0;
	}

	public final void setRange(double low0, double high0) {
		low = low0;
		high = high0;
	}
	
	public final void setValue(double value) {
		low = high = value;
	}
}