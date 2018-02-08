package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private int count;
    private TupleDesc td;
    private boolean inserted;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
    	// some code goes here
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
        this.inserted = false;
        
        String[] name = new String[] {"Inserted"};
	    	Type[] type = new Type[] {Type.INT_TYPE};
	    	this.td = new TupleDesc(type, name);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    		super.open();
    		this.child.open();
    		this.inserted = false;
    		count = 0;
    }

    public void close() {
        // some code goes here
    		super.close();
    		this.child.close();
    		this.inserted = false;
    		count = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		this.child.rewind();
    		this.inserted = false;
    		count = 0;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		if(this.child == null || this.inserted) return null;
    		while(this.child.hasNext()) {
    			try {
					Database.getBufferPool().insertTuple(this.tid, this.tableid, child.next());
				} catch (NoSuchElementException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
    			count++;
    		}
    		this.inserted = true;
        	Tuple ret = new Tuple(this.td);
        	ret.setField(0, new IntField(count));
        	return ret;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    		DbIterator ret[] = new DbIterator[] {this.child};
        return ret;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    		this.child = children[0];
    }
}
