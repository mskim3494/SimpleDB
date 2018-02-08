package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
	
	//declared class variables
	private int tableId;
	private int pgNo;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
    	//store the parameters passed into the constructor
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
    		//concatenate tableId and pageNo as required
    		String temp = "" + this.tableId + this.pgNo;
    		return temp.hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
    	//if the object is non-null and a PageId object, it is valid for a comparison
    	if(o != null && o instanceof PageId) {
    		//A PageId is defined with tableId and pgNo, so comoare the two values.
    		if(this.tableId == ((PageId) o).getTableId() && this.pgNo == ((PageId) o).getPageNumber()) {
	       		return true;
	       	}
    	}
    	//if the other object is null or not a PageId object, not equal, return false.
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
