package simpledb;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<Field, Integer> gbfields;
    private HashMap<Field, Integer> gbcount; // solely for keeping track for AVG
    
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.gbfields = new HashMap<Field, Integer>();
        this.gbcount = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = null;
        if(gbfield != Aggregator.NO_GROUPING) {
        		field = tup.getField(gbfield);
        }
        // initialize the field map
        if(!gbfields.containsKey(field)) {
        		int init = 0;
        		switch(this.what) {
        			case MIN: 
        				init = Integer.MAX_VALUE; // any value will be less than max
        				break;
        			case MAX: 
        				init = Integer.MIN_VALUE; // any value will be greater than min
        				break;
        			default: 
        				init = 0; // sum, count, avg all start at 0
        		}
        		gbfields.put(field, init);
        		gbcount.put(field, 0);
        } 
        int currVal = gbfields.get(field);
        int tupleVal = ((IntField)tup.getField(afield)).getValue();
        switch(this.what) {
	        case MIN: 
	        		if(tupleVal < currVal) {
	        			currVal = tupleVal;
	        		} break;
	        case MAX:
		        	if(tupleVal > currVal) {
		    			currVal = tupleVal;
		    		} break;
	        case SUM:
	        		currVal += tupleVal;
	        		break;
	        case AVG:
	        		currVal += tupleVal;
	        		gbcount.put(field, gbcount.get(field)+1);
	        		break;
	        case COUNT:
	        		currVal++;
	        		break;
	        	default: // shouldn't get here
	        		break;
        }
        gbfields.put(field, currVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>(); //tuples to return
        TupleDesc desc;
        String[] names;
	    	Type[] types;
	    	if (gbfield == Aggregator.NO_GROUPING){
	    		names = new String[] {"aggregateVal"};
	    		types = new Type[] {Type.INT_TYPE};
	    	} else {
	    		names = new String[] {"groupVal", "aggregateVal"};
	    		types = new Type[] {gbfieldtype, Type.INT_TYPE};
	    	}
	    	desc = new TupleDesc(types, names);
	    	
	    	Tuple toAdd;
	    	Iterator<Map.Entry<Field, Integer>> it = gbfields.entrySet().iterator();
	    	Map.Entry<Field, Integer> nextfield;
	    	int aggregateVal = 0;
	    	while(it.hasNext()) {
	    		nextfield = it.next();
	    		if(what == Op.AVG) {
	    			aggregateVal = nextfield.getValue() / gbcount.get(nextfield.getKey());
	    		} else {
	    			aggregateVal = nextfield.getValue();
	    		}
	    		toAdd = new Tuple(desc);
	    		if(gbfield == Aggregator.NO_GROUPING) {
	    			toAdd.setField(0, new IntField(aggregateVal));
	    		} else {
	    			toAdd.setField(0, nextfield.getKey());
	    			toAdd.setField(1, new IntField(aggregateVal));
	    		}
	    		tuples.add(toAdd);
	    	}
	    	return new TupleIterator(desc, tuples);
    }
}
