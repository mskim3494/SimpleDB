package simpledb;

import java.util.ArrayList; //added

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private int buckets;
	private int min;
	private int max;
	private double binSize; //width of each bin
	private int ntups; //number of values to be histogrammed
	private ArrayList<Integer> hist; 
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
	    	// some code goes here
	    	this.buckets = buckets;
	    	this.min = min;
	    	this.max = max;
	    	this.binSize = (max-min)/ (double)buckets;
	    	
	    	//used ArrayList instead of fixed-size array because
	    	//a class variable with fixed-size array can't be declared
	    	//before num of buckets is passed in here.
	    	hist = new ArrayList<Integer>();
	    	for (int i=0; i<buckets; i++)
	    		hist.add(0);
	    	this.ntups = 0;
    }
    
    
    
    /**
     * Helper function used to
     * find the bin index of the value being passed in
     */

    //e.g. if you have 2 ~ 10002 with 1000 buckets. width 10 each
    //how to get 8888?
    //2 + 10*888 = 8882. so index 888 out of 1000
    //8888-2/10 = 888.6. so just apply floor function
    
    //exception: with 10002 you get 10002-2/10 = 1000, index out of bounds
    public int getBinIndex(int v) {
	    	if (v == this.max)
	    		return this.buckets-1;
	    	else if (this.min <= v && v < this.max)
	    		return (int) Math.floor((v - this.min) / this.binSize);
	    	else if (v < this.min)
	    		return -1; //value smaller than valid range
	    	else
	    		return -2; //value larger than valid range
    }
    
    
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
	    	int index = getBinIndex(v);
	    	hist.set(index, hist.get(index) + 1); //reset ith value with 1 added
	    	this.ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
	    	int index = getBinIndex(v);
	    	double b_f;
	    	double b_part;
	    	double b_right;
	    	double selectivity = 0.0;
	    	
	    	if (op == Op.EQUALS || op == op.NOT_EQUALS) {
	    		// (h/w) / ntups
	    		if ((index == -1 || index == -2) && op == op.EQUALS)
	    			return 0.0;
	    		if ((index == -1 || index == -2) && op == op.NOT_EQUALS)
	    			return 1.0;
	    		if (op == op.EQUALS) {
	    			return (double) (this.hist.get(index)) / this.ntups;
	    		} else
	    			return 1 - (double) (this.hist.get(index)) / this.ntups;
	    	}
    	
	    	// I don't think equality makes any difference here?
	    	//0-6 bin with bin width 6 height 6, containing integers 0, 1, 2, 3, 4, 5
	    	//assume ntups = 12. so b_f = 6/12 = 0.5
	    	// >3: b_f * b_part = 6-3/6 = 50%, which already includes 3.
	    	// so >=3 would be the same.
	    	else if (op == Op.GREATER_THAN || op == Op.GREATER_THAN_OR_EQ ||
	    			 op == Op.LESS_THAN || op == Op.LESS_THAN_OR_EQ) {
	    		//b_f = h_b / ntups
	    		if (index != -1 && index != -2) {
		    		b_f = (double) this.hist.get(index) / this.ntups;
		    		b_right = min + (index+1)*this.binSize; //right, so left of next bin
		    		// (b_right - const) / w_b
		    		b_part = (double) (b_right - v) / this.binSize;
		    		selectivity += b_f * b_part;
	    		}
	    		//some additional edge cases
	    		if (v == this.min && op == Op.LESS_THAN) 
	    			//although index would be 0, < this.min would not return any
	    			return 0.0;
	    		if (v == this.max && op == Op.GREATER_THAN) 
	    			//although last index would be returned, > this.max would not return any
	    			return 1.0;

	    		
	    		if (op == Op.GREATER_THAN || op == Op.GREATER_THAN_OR_EQ) {
	    			if (index == -1) 
	    				//if value is smaller than min, all values are greater than that
	    				return 1.0;
	    			if (index == -2) 
	    				//if value is larger than max, no value can be greater than that
	    				return 0.0;
	    			
	    			//for all buckets to the right
		    		for (int i = index+1; i < this.buckets; i++) {
		    			//b_f = h_b / ntups. without applying b_part since the whole bin is added
		    			selectivity += this.hist.get(i) / (double) this.ntups;
		    		}
	    		}
	    		else { //less and less_than_or_eq
	    			if (index == -1) 
	    				//if value is smaller than min, no value can be smaller than that
	    				return 0.0;
	    			if (index == -2) 
	    				//if value is larger than max, all values can be smaller than that
	    				return 1.0;
	    			
	    			//for all buckets to the left
		    		for (int i = index-1; i >= 0; i--) {
		    			//b_f = h_b / ntups. without applying b_part since the whole bin is added
		    			selectivity += this.hist.get(i) / (double) this.ntups;
		    		}    			
	    		}
	    	
	    	}
	    	
	    	//Op.LIKE is not dealt with here, because LIKE for Integer doesn't make sense
	    	// some code goes here
	    	return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
	    	String out = "\nntups: " + String.valueOf(this.ntups) + "\n";
	    	for (int i=0; i<this.buckets; i++) {
	    		out +=	String.valueOf(this.min + i * this.binSize) + "~" + 
	    				String.valueOf(this.min + (i+1) * this.binSize) + " -> " + 
	    				String.valueOf(this.hist.get(i)) + "\n";
	    	}
	    	return out;
    }
}
