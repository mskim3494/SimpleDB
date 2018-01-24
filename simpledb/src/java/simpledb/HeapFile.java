package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc td;
	private int tableid;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.tableid = this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.tableid;
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
	    	int offset = BufferPool.getPageSize() * pid.getPageNumber();	
	    	byte[] buffer = new byte[BufferPool.getPageSize()];
	    	RandomAccessFile raf = null;
	    	Page ret = null;
		try {
			raf = new RandomAccessFile(this.file,"r");
		    	//assert (offset + BufferPool.getPageSize() < raf.length());
		    	raf.seek(offset);
		    	raf.read(buffer);
		    	ret = new HeapPage((HeapPageId) pid, buffer); 
		    	return ret;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				raf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// shouldn't get here
		return ret;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HfIterator(this, tid);
    }
    
    // Subclass for DbFileIterator
    class HfIterator extends AbstractDbFileIterator{
    		private HeapFile hf;
    		private TransactionId tid;
    		private Iterator<Tuple> tuples;
    		private HeapPage currPage;
    		private int currPageNo;
    		
    		HfIterator(HeapFile hf, TransactionId tid){
    			//System.out.println("HF: HfIterator constructor");
    			this.hf = hf;
    			this.tid = tid;
    			this.currPage = null;
    			this.tuples = null;
    			this.currPageNo = 0;
    			
    			
    			//since this.open() does not seem to have run...
    			//after adding this, some testSmall cases pass.
    			try {
					open();
				} catch (DbException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TransactionAbortedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			
    		}
    		
		@Override
		public void open() throws DbException, TransactionAbortedException {
			//System.out.println("HF: HFIterator.open run");
			HeapPageId currpid = new HeapPageId(this.hf.getId(), this.currPageNo);
            this.currPage = (HeapPage) Database.getBufferPool().getPage(this.tid, currpid, null);
            this.tuples = this.currPage.iterator();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			this.close();
			this.open();
		}

		@Override
		protected Tuple readNext() throws DbException, TransactionAbortedException {
			//System.out.println("HFI.readnext");
			if (this.tuples != null) {
				//System.out.println("this.tuples is not null");
				if(this.tuples.hasNext()) {
					//System.out.println("this.tuples has next");
					return this.tuples.next();
				} else {
					//System.out.println("this.tuples does not have next");
					boolean breakflag = true;
					while(breakflag) {
						if(this.currPageNo < this.hf.numPages() - 1) {
							HeapPageId currpid = new HeapPageId(this.hf.getId(), ++this.currPageNo);
							this.currPage = (HeapPage) Database.getBufferPool().getPage(this.tid, currpid, null);
				            this.tuples = this.currPage.iterator();
				            if(this.tuples != null && this.tuples.hasNext()) {
				            		return this.tuples.next();
				            } 
						} else {
							return null;
						}
					}
				}
			}
			//else {
			//	System.out.println("this.tuples == null");
			//}
			return null;
		}
		
		@Override
		/** If subclasses override this, they should call super.close(). */
	    public void close() {
	        super.close();
	        this.tuples = null;
	        this.currPage = null;
	        this.currPageNo = 0;
	    }
    		
    }

}

