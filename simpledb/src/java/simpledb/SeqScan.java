package simpledb;

import java.util.*;

//added
import simpledb.HeapFile.HfIterator;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    private HfIterator i = null;
    private TupleDesc td = null;
    private int tableid;
    private String tableAlias;
    private TransactionId tid;
    
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this.tid = tid;
    	this.tableid = tableid;  	
    	this.tableAlias = tableAlias;
    }
    
    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
    	return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
    	this.tableid = tableid;  	
    	this.tableAlias = tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
		// get a heapfile associated with this tableid from catalog
		HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableid);
		// borrow from HeapFile's iterator and run its open() method.
		this.i = (HfIterator) hf.iterator(this.tid);
		this.i.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	return Database.getCatalog().getTupleDesc(this.tableid);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.i.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return this.i.next();
    }

    public void close() {
    		this.i.close();
    		this.i = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	close();
    	open();
    }
}
