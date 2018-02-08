package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private int count;
    private TupleDesc td;
    private boolean deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    		this.tid = t;
    		this.child = child;
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
    		this.count = 0;
    		this.deleted = false;
    }

    public void close() {
        // some code goes here
    		super.close();
    		this.child.close();
    		this.count = 0;
    		this.deleted = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		this.child.rewind();
    		this.count = 0;
    		this.deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
		if(child == null || this.deleted) return null;
		while(this.child.hasNext()) {
			try {
				Database.getBufferPool().deleteTuple(this.tid, this.child.next());;
			} catch (NoSuchElementException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			this.count++;
		}
		this.deleted = true;
	    	Tuple ret = new Tuple(this.td);
	    	ret.setField(0, new IntField(this.count));
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
