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
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
		//prepare an offset to start reading the file with
    		int offset = BufferPool.getPageSize() * pid.getPageNumber();
    		//buffer to store the reads
    		byte[] buffer = new byte[BufferPool.getPageSize()];
    		RandomAccessFile raf = null;
    		Page ret = null;
		try {
			raf = new RandomAccessFile(this.file,"r");
		    	raf.seek(offset); //move by buffer 
		    	raf.read(buffer); //read into the buffer
		    	ret = new HeapPage((HeapPageId) pid, buffer); //create a new HeapPage 
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
    		int pageNo = page.getId().getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        RandomAccessFile raf = null;
        raf = new RandomAccessFile(this.file, "rw");
        raf.seek(offset);
        raf.write(data,0,BufferPool.getPageSize());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // formula as suggested in lab1.md
        return (int) Math.ceil((double) this.file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
	    	int pageNo = 0;
	    	HeapPage hp = null;
	    	HeapPageId hpid = null;
	    	// a stupid approach of trying to iterate over pages.. even if not in buffer pool
	    	for (pageNo = 0; pageNo < this.numPages(); pageNo++) {
	    		hpid = new HeapPageId(this.tableid, pageNo);
	    		hp = (HeapPage) Database.getBufferPool().getPage(tid, hpid, null);
	    		if (hp.getNumEmptySlots() != 0) {
	    			//now we have an hp with at least one empty slot
	    	    		hp.insertTuple(t);
	    	    		ArrayList<Page> al = new ArrayList<Page>();
	    		    	al.add(hp); //only one page will be added.. right? for a single tuple insertion
	    	        return al;
	    		}
	    	}
	    	// need new page
	    	HeapPageId newPageId = new HeapPageId(this.getId(), this.numPages());
        HeapPage newHP = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        newHP.insertTuple(t);
        writePage(newHP);
        ArrayList<Page> al = new ArrayList<Page>();
        al.add(newHP);
        return al;
	    	
        
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    		// similar to insertTuple, use HeapPage implementation to deleteTuple
	    	PageId pid = t.getRecordId().getPageId();
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
        hp.deleteTuple(t);
        ArrayList<Page> al = new ArrayList<Page>();
        al.add(hp);
        return  al;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HfIterator(this, tid);
    }
    
    // Subclass for DbFileIterator
    class HfIterator extends AbstractDbFileIterator{
    		// tid not used in lab1 but included for future use
    		// the heapfile is saved so that it can be read
    		// currPage and currPageNo are for tracking purposes
    		private HeapFile hf;
    		private TransactionId tid;
    		private Iterator<Tuple> tuples;
    		private HeapPage currPage;
    		private int currPageNo;
    		
    		HfIterator(HeapFile hf, TransactionId tid){
    			this.hf = hf;
    			this.tid = tid;
    			this.currPage = null;
    			this.tuples = null;
    			this.currPageNo = 0;
    		}
    		
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// open the iterator, get the first page. currPageNo should be 0
			HeapPageId currpid = new HeapPageId(this.hf.getId(), this.currPageNo);
            this.currPage = (HeapPage) Database.getBufferPool().getPage(this.tid, currpid, null);
            this.tuples = this.currPage.iterator();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// closing gets rid of the iterator and sets everything back to original
			this.close();
			this.open();
		}

		@Override
		protected Tuple readNext() throws DbException, TransactionAbortedException {
			// read tuples. Makes use of the already implemented hasNext and next methods
			if (this.tuples != null) {
				if(this.tuples.hasNext()) {
					return this.tuples.next(); // if there is a next tuple, simply return it
				} else {
					// will have to move to next page, but since next page might also be empty
					// iterate until find a page with data or reached the max number of pages
					boolean breakflag = true;
					while(breakflag) {
						if(this.currPageNo < this.hf.numPages() - 1) {
							HeapPageId currpid = new HeapPageId(this.hf.getId(), ++this.currPageNo);
							this.currPage = (HeapPage) Database.getBufferPool().getPage(this.tid, currpid, null);
				            this.tuples = this.currPage.iterator();
				            if(this.tuples != null && this.tuples.hasNext()) {
				            		return this.tuples.next();
				            } 
						} else { // no tuples in remaining pages
							return null;
						}
					}
				}
			}
			return null;
		}
		
		//added because it seems necessary to pass BufferPoolWriteTest.handleManyDirtyPages
		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			//System.out.println("HFI.hasNext");
			if (this.tuples != null) {
				//System.out.println("this.tuples not null");
				if(this.tuples.hasNext()) {
					//System.out.println("has next");
					return true; // if there is a next tuple, simply return it
				} else {
					//System.out.println("doesn't have next");
					// will have to move to next page, but since next page might also be empty
					// iterate until find a page with data or reached the max number of pages
					boolean breakflag = true;
					while(breakflag) {
						//System.out.println("HF's numpages: " + this.hf.numPages());
						if(this.currPageNo < this.hf.numPages() - 1) {
							HeapPageId currpid = new HeapPageId(this.hf.getId(), ++this.currPageNo);
							//System.out.println("will try to get a page " + currpid.getPageNumber() + " from buffer");
							this.currPage = (HeapPage) Database.getBufferPool().getPage(this.tid, currpid, null);
				            //System.out.println("empty slots for page " + this.currPage.getId().getPageNumber() + " " + this.currPage.getNumEmptySlots());
							this.tuples = this.currPage.iterator();
				            //System.out.println("tuples updated to currpid: " + currpid.getPageNumber());
				            if(this.tuples != null && this.tuples.hasNext()) {
				            		//System.out.println("returning next");
				            		return true;
				            } 
						} else { // no tuples in remaining pages
							return false;
						}
					}
				}
			}
			return false;
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

