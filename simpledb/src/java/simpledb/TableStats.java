package simpledb;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.TupleDesc.TDItem;
import java.util.ArrayList; //newly added


/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    private ArrayList<IntHistogram> intHists;
    private ArrayList<StringHistogram> stringHists;
    
    private TupleDesc td;
    //this is needed so that when you want to get field 5, for example,
    //you know which index in one of the histograms to look for
    //e.g. we would know whether the field is int or string,
    //if the field is int, what index should we look for in intHists? (say 2)
    //so this would store 0, 0, 1, 2, 1, for example, for int0, str0, str1, str2, str1
    private ArrayList<Integer> histIndices;
    
    
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    	//first, need to initialize HeapFile for tableid
    	HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
    	this.td = td;
    	Iterator<TDItem> it = td.iterator(); //each TDItem has .fieldName and .fieldType
    	
    	//will probably need to build a histogram (either Int or String) for each column
    	//StringHistogram is based on IntHistogram, and I think the sizes are the same
    	this.intHists = new ArrayList<IntHistogram>();
    	this.stringHists = new ArrayList<StringHistogram>();
    	this.histIndices = new ArrayList<Integer>();
    	
    	//Step 1. First scan of table. to find min and max of each integer column    	
    	int intMinMax[] = new int[2*td.numFields()]; //will store min and max
    	int intindex = 0;
    	int counter = 0; //to distinguish first tuple from all the rest
    	
    	//initialize & open dbit.
    	DbFileIterator dbit = hf.iterator(new TransactionId());
    	try {
			dbit.open();
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
		}

    	
    	try {
			while (dbit.hasNext()) { //for each tuple
				Tuple tup = dbit.next();
				
				for (int i=0; i<td.numFields(); i++) { //for each attribute
					
					if (td.getFieldType(i) == Type.INT_TYPE) { 
						IntField f = (IntField) tup.getField(i);
						int val = f.getValue();
						
						if (counter == 0) { //for the first read, set this column's min and max both as val
							intMinMax[2*intindex] = val; //for min for this column
							intMinMax[2*intindex + 1] = val; //for max for this column
						}
						else {
							if (val < intMinMax[2*intindex]) //if val < min, min = val
								intMinMax[2*intindex] = val;
							if (val > intMinMax[2*intindex + 1]) // if val > max, max = val
								intMinMax[2*intindex + 1] = val;
						}
						intindex++;

					}
					else {
						//no need to get min max stringval, because
						//in StringHistogram they just set "" and "zzzz" converted to integer as min and max
					}

				}
				
				intindex=0; //after one tuple read, reset this.
				counter++; //also update row counter
			}
		} catch (NoSuchElementException | DbException | TransactionAbortedException e) {
			e.printStackTrace();
		}
    	dbit.close();
    	
    	//Step 2. initialize Histograms for each column
    	intindex = 0;
    	for (int i=0; i<td.numFields(); i++) {
    		if (td.getFieldType(i) == Type.INT_TYPE) {
    			intHists.add(new IntHistogram(NUM_HIST_BINS, intMinMax[2*intindex], intMinMax[2*intindex+1]));
    			intindex++;
    		}
    		else
    			stringHists.add(new StringHistogram(NUM_HIST_BINS)); //min, max val already set as default
    	}
    	
    	
    	//Step 3. Second scan: addValue() for each Hist that is empty up to this point
    	//also update histindices for convenience
    	//reinitialize dbit, and open
    	dbit = hf.iterator(new TransactionId());
    	try {
			dbit.open();
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
		} 
    	
    	intindex = 0;
    	int stringindex = 0; //this time, we also need to locate stringHistogram correctly
    	
		try {
			while (dbit.hasNext()) { //for each tuple
				Tuple tup = dbit.next();
				
				for (int i=0; i<td.numFields(); i++) { //for each attribute
					if (td.getFieldType(i) == Type.INT_TYPE) { //int 
						//get integer value and add it to histogram
						IntField f = (IntField) tup.getField(i);
						int val = f.getValue(); 
						intHists.get(intindex).addValue(val);
						this.histIndices.add(intindex);
						intindex++;
					}
					else {
						//get string value and add it to histogram
						StringField f = (StringField) tup.getField(i);
						String val = f.getValue();
						stringHists.get(stringindex).addValue(val);
						this.histIndices.add(stringindex);
						stringindex++;
					}
				}
				
				intindex = 0;
				stringindex = 0; //after one tuple read, reset these
			}
		} catch (NoSuchElementException | DbException | TransactionAbortedException e) {
			e.printStackTrace();
		}
    	
		
		//Step 4. now, based on the histograms, need to calculate table statistics 
		// -> or is this step even needed? IntHistogram already has estimateSelectivity
		
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return 0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return 0;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	//for example for int0, int1, str1, int2, int3, str2, str3
    	//we have 0, 1, 1, 2, 3, 2, 3 for histIndices
    	//so getting field 4(int2) -> index 2 from IntHistogram
    	int index = this.histIndices.get(field);
    	
    	if (this.td.getFieldType(field) == Type.INT_TYPE) {
    		int val = ((IntField) constant).getValue();
    		return this.intHists.get(index).estimateSelectivity(op, val);
    	}
    	else {
    		String val = ((StringField) constant).getValue();
    		return this.stringHists.get(index).estimateSelectivity(op, val);    		
    	}

    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
