package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

//added

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId rid;
    private ArrayList<Field> fields;

    
    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td.numFields() >= 1){
            this.td = td;
            this.fields = new ArrayList<Field>(td.numFields());
            for (int i=0; i<td.numFields(); i++)
            	this.fields.add(null); //adding nulls as a placeholder: arraylist is originally of 0 size
            
        }
        System.out.println("after init, tuple's size: " + this.fields.size() + " " + td.numFields());

    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if(i < td.numFields()){
            this.fields.set(i, f);
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if(i < td.numFields()){
            return fields.get(i);
        } else {
            return null;
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        int isize = td.numFields();
        String ret = "";
        for(int i=0; i< isize; i++){
            ret.concat(fields.get(i).toString()).concat("\t");
        }
        return ret;
        //throw new UnsupportedOperationException("Implement this");
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return fields.iterator();
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        if (td.numFields() >= 1){
            this.td = td;
            this.fields = new ArrayList<Field>(td.numFields());
        }
    }
}
