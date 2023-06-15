package edu.rit.gdb.a8;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class LatinHypercube {
	
	class Interval {
		// Initial and end value of the interval; endValue-initialValue=1/n, where n is the number of points.
		BigDecimal initialValue, endValue;
		// The scaled values taking the dimension into account. These can be integers (dim, batchSize, negativeRate),
		//	BigDecimal (alpha, gamma) or String (distance).
		Object scaledInitialValue, scaledEndValue;
	}

	public static void main(String[] args) throws Exception {
		final int points = Integer.valueOf(args[0]);
		final long seed = Long.valueOf(args[1]);
		final String outputFile = args[2];
		
		// Dimensions: 6; one for each hyperparameter.
		// Hyperparameters:
		// - Dimension 0: dim in [5, 25], integer; the convention is RoundingMode.HALF_DOWN.
		// - Dimension 1: batchSize in [100, 1000], integer; the convention is RoundingMode.HALF_DOWN.
		// - Dimension 2: negativeRate in [1, 20], integer; the convention is RoundingMode.HALF_DOWN.
		// - Dimension 3: alpha in [1e-10, 1.0], BigDecimal.
		// - Dimension 4: gamma in [.01, 10.], BigDecimal.
		// - Dimension 5: distance in {L1, L2}, String; the convention is <.5 => L1; >=.5 => L2.
		int dim = 6;
		// Rows are the points, columns are the dimensions, each entry is an Interval object (see above).
		Table<Integer, Integer, Interval> latinHypercubeIntervals = HashBasedTable.create();
		LatinHypercube aux = new LatinHypercube();
		Random rnd = new Random(seed);
		
		// TODO Your code here!!!!
		// In a Latin hypercube, each point has a number of dimensions, each of which is a hyperparameter value
		//	in our case. We are going to assign each dimension at once. We divide each dimension into n intervals
		//	of 1/n size, where n is the number of points. Intervals are shuffled using the seed provided as input.
		
		for (int d = 0; d < dim; d++) {
			List<Interval> intervals = new ArrayList<>();
			float start = 0;
			float end = 0;
			
			// Create the intervals.
			for (int i = 0; i < points; i++) {
				Interval v = aux.new Interval();

				// TODO Provide the v attributes here.

				start = (float) (i / points);
				end  = (float) ((i+1) / points);

				v.initialValue = BigDecimal.valueOf(start);
				v.endValue = BigDecimal.valueOf(end);

				// TODO SCALE THE VALUES

				// TODO Ends here.
				
				intervals.add(v);
			}
			
			// Shuffle.
			Collections.shuffle(intervals, rnd);
			
			// Assign intervals: point i gets interval i.
			for (int i = 0; i < points; i++)
				latinHypercubeIntervals.put(i, d, intervals.get(i));
		}
		
		Table<Integer, Integer, BigDecimal> latinHypercubeSelection = HashBasedTable.create();
		// TODO Compute points within each interval. We select the middle point of the interval.
		Interval currInterval;
		for (int d = 0; d < dim; d++)
			for (int i = 0; i < points; i++){
				currInterval = latinHypercubeIntervals.get(i, d);
				BigDecimal midPoint = new BigDecimal(0);
				if (d == 0 || d == 1 || d == 2){
				midPoint = BigDecimal.valueOf((int)currInterval.scaledInitialValue).add(BigDecimal.valueOf((int)currInterval.scaledEndValue));
				midPoint = midPoint.divide(new BigDecimal(2), RoundingMode.HALF_DOWN);
				}
				else if (d == 3 || d == 4){
					midPoint = ((BigDecimal)currInterval.scaledInitialValue).add((BigDecimal)currInterval.scaledEndValue);
					midPoint = midPoint.divide(new BigDecimal(2), RoundingMode.HALF_DOWN);
				}
				else{
					midPoint = BigDecimal.valueOf((int)currInterval.scaledInitialValue).add(BigDecimal.valueOf((int)currInterval.scaledEndValue));
					midPoint = midPoint.divide(new BigDecimal(2), RoundingMode.HALF_DOWN);
				}
				latinHypercubeSelection.put(i, d, midPoint /* TODO Middle point of the (i, d) interval */);
			}
		
		// TODO Compute MD^2: Eq. 18 in Yong-Dao Zhou, Kai-Tai Fang, Jian-Hui Ning, Mixture discrepancy for quasi-random point sets, Journal of Complexity,
		//	Volume 29, Issues 3-4, 2013, Pages 283-301, ISSN 0885-064X, https://doi.org/10.1016/j.jco.2012.11.006.
		BigDecimal md = null;
		
		// TODO End of your code.
		
		PrintWriter writer = new PrintWriter(new File(outputFile));
		// First line: intervals.
		for (int i = 0; i < points; i++) {
			writer.print(i);
			writer.print(":");
			for (int d = 0; d < dim; d++) {
				Interval v = latinHypercubeIntervals.get(i, d);
				writer.print(removeLastDigits(v.initialValue));
				writer.print(",");
				writer.print(removeLastDigits(v.endValue));
				writer.print(",");
				if (d == 3 || d == 4)
					writer.print(removeLastDigits((BigDecimal) v.scaledInitialValue));
				else
					writer.print(v.scaledInitialValue);
				writer.print(",");
				if (d == 3 || d == 4)
					writer.print(removeLastDigits((BigDecimal) v.scaledEndValue));
				else
					writer.print(v.scaledEndValue);
				writer.print(",");
			}
			writer.print(";");
		}
		writer.println();
		
		// Second line: selection.
		for (int i = 0; i < points; i++) {
			writer.print(i);
			writer.print(":");
			for (int d = 0; d < dim; d++) {
				BigDecimal v = latinHypercubeSelection.get(i, d);
				writer.print(removeLastDigits(v));
				writer.print(",");
			}
			writer.print(removeLastDigits(md));
			writer.print(";");
		}
		writer.println();
		
		writer.close();
	}
	
	private static String removeLastDigits(BigDecimal e) {
		return e.setScale(20, RoundingMode.HALF_DOWN).toString();
	}

}
