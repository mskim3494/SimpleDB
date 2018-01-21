package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> tditems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
      	Iterator<TDItem> itr = tditems.iterator();
        return itr;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
      // some code goes here
      this.tditems = new ArrayList<TDItem>();	
    	
      //System.out.println("initializing new TD, length of arrays: " + typeAr.length + " " + fieldAr.length);
      for (int i=0; i<typeAr.length; i++) {
    	//System.out.println(i);
    	 
    	//System.out.println(i + " " + typeAr[i] + " " + fieldAr[i]);
        tditems.add(new TDItem(typeAr[i], fieldAr[i]));
       
      }
      //System.out.println("final size: " + tditems.size());
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this.tditems = new ArrayList<TDItem>();
      for (int i=0; i<typeAr.length; i++) {
        tditems.add(new TDItem(typeAr[i], null));
      }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tditems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return tditems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tditems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
      for (int i=0; i<this.numFields(); i++) {
    	if (this.getFieldName(i) != null) 
    		if (this.getFieldName(i).equals(name)) //if the ith fieldname matches the fn arg 'name',
    			return i;
      }
      //if no matching fieldName existed, throw an exception
      throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
      int isize = 0;
      for(int i=0; i<tditems.size(); i++){
        isize += tditems.get(i).fieldType.getLen();
      }
      	return isize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
      // some code goes here
      //System.out.println("numfields of td1 and td2: " + td1.numFields() + " " + td2.numFields());
      int totalNumFields = td1.numFields() + td2.numFields();
      Type[] typeAr = new Type[totalNumFields];
      String[] fieldAr = new String[totalNumFields];
      
      //System.out.println("size of to-be-merged array: " + totalNumFields);
      //fill in typeAr and fieldAr with one TDItem at a time, starting from td1
      for(int i=0; i < td1.numFields(); i++) {
    	//System.out.println("i: "+ i);
        TDItem item = td1.tditems.get(i);
        typeAr[i] = item.fieldType;
        fieldAr[i] = item.fieldName;
        //System.out.println(item.fieldType + " " + item.fieldName + " added to " + i);
      }
      for(int i=0; i < td2.numFields(); i++) {
    	
        TDItem item = td2.tditems.get(i);
        typeAr[td1.numFields() + i] = item.fieldType;
        fieldAr[td1.numFields() + i] = item.fieldName;
        //System.out.println(item.fieldType + " " + item.fieldName + " added to " + (int)(td1.numFields() + i));
      }
      

      /*
      for (int i = 0; i < td1.numFields(); i++) {
    	  System.out.println("td1-" + i + " " + td1.getFieldName(i) + " " + td1.getFieldType(i));
      }
      for (int i = 0; i < td2.numFields(); i++) {
    	  System.out.println("td2-" + i + " " + td2.getFieldName(i) + " " + td2.getFieldType(i));
      }
      
      System.out.println("before initializing, size of arrays: " + typeAr.length + " " + fieldAr.length);
      */
      
      
      TupleDesc newTd = new TupleDesc(typeAr, fieldAr);
      
      
      return newTd;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
      
      if (o instanceof TupleDesc) {
	      if (this.numFields() == ((TupleDesc) o).numFields() ) {
	        Iterator<TDItem> td1iter = this.iterator();
	        Iterator<TDItem> td2iter = ((TupleDesc) o).iterator();
	        while(td1iter.hasNext() && td2iter.hasNext()){
	          TDItem item1 = td1iter.next();
	          TDItem item2 = td2iter.next();
	          if(item1.fieldType != item2.fieldType) {
	            return false; //if the fieldtype doesn't match
	          }
	        } 
	        return true; //if all fieldTypes were equal, return
	      } 
	      else 
	        return false; //if the num of fields do not match
      }
      else
    	  return false; //if object is not TupleDesc
    }


    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
      // some code goes here
      String descStr = "";
      while(this.iterator().hasNext()){
       	 descStr += this.iterator().next().toString(); //using TDItem's toString method that returns a string for each field.
      }
      return descStr;
    }
}
