package simpledb;

import java.util.*;
import java.io.*;
import java.lang.Math;


/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[]; //to store header bits indicating whether a slot is empty
    final Tuple tuples[];
    final int numSlots; //number of tuple slots this page can store
    
    private TransactionId tid; // dirty mark

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.tid = null;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate a proper number of bytes for header[]
        header = new byte[getHeaderSize()];
        //read the header slots of this page from dis.
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        // allocate a proper number of bytes for tuples[]
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        } catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
    	// formula as suggested in lab1.md
    	return (int) Math.floor(BufferPool.getPageSize()*8) / (td.getSize() * 8 + 1);	
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
    	// formula as suggested in lab1.md
    	return (int)Math.ceil(this.numSlots / 8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    		return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.	
    		if (!isSlotUsed(slotId)) {
	        for (int i=0; i<td.getSize(); i++) {
	                try {
	                    dis.readByte();
	                } catch (IOException e) {
	                    throw new NoSuchElementException("error reading empty tuple");
	                }
	            
	            return null;
	        }
    		}
        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
            		Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    		RecordId todelete = t.getRecordId();
    		// tuple integrity check
    		if (todelete == null || !this.pid.equals(todelete.getPageId())){
        		throw new DbException("Tuple is empty or not on page.");
        	}
    		int tupleno = todelete.getTupleNumber();
    		// is tuple valid
    		if (tupleno < 0 || tupleno >= numSlots) {
    			throw new DbException("Tupleno is wrong.");
    		} // is tuple on page
    		if(!isSlotUsed(tupleno)) {
    			throw new DbException("Tupleno is already deleted.");
    		}
    		this.markSlotUsed(tupleno, false);
    		this.tuples[tupleno] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    		if (getNumEmptySlots() == 0 || !(t.getTupleDesc().equals(this.td)))
            throw new DbException("HeapPage is full or TupleDesc does not match.");
        int i = 0;
	    for (i = 0; i < this.numSlots; i++) {
	    		if (!isSlotUsed(i)) {
	            markSlotUsed(i, true);
		        tuples[i] = t;
		        tuples[i].setRecordId(new RecordId(pid, i));
		        break;
	    		}
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
    		// not necessary for lab1
    		if (dirty) {
    			this.tid = tid;
    		} else {
    			this.tid = null;
    		}
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
    	// Not necessary for lab1
        return this.tid;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
    	// count the number of 1-bits in the header
    	int onebits = 0;
    	for (int i=0; i<header.length; i++) { //for each byte in header
    		byte abyte = header[i];
    		for (int j=0; j<8; j++)  {//for each bit of the byte
    			
    			//the bit index (i*8+j) should be less than the num of tuples,
    			//as the last bit used in the header would have index numSlots-1 
    			if (i*8 + j < this.numSlots) {
    				//create a mask and perform & operation to filter out the bit of interest
    				//then, right shift back to get either 0 or 1 only.
	    			int bit = ((int) abyte & (1 << j)) >> j;
	    			onebits += bit; //add 0 or 1 to the count
    			}
    		}
    		
    		
    	}
    	
    	//total num of slots - occupied slots = num of empty slots
        return this.numSlots - onebits;
        
        
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
	    	//given i is the slot index, split the information into
	    	//the byteindex in the header and which bit in the byte is the correct one
	    	int headerByteIndex = (int)(i / 8);
	    	int bitIndex = i % 8;
	    	//then, using that information, extract the proper bit indicating
	    	//whether the tuple slot is currently used
	    	int abyte = header[headerByteIndex];    	
	    	int bit = ((int) abyte & (1 << bitIndex)) >> bitIndex;
        return bit == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    		int index = i / 8;
        int offset = i % 8;
        if (value) {
        		header[index] |= (1 << offset);
        } else {
        		header[index] &= (~(1 << offset));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        
    	//to build an iterator, an iterable needs to be created
    	//initialize an ArrayList and start adding tuples
    	ArrayList<Tuple> al = new ArrayList<Tuple>();
    	for (int i=0; i<this.numSlots;i++) {
    		if (isSlotUsed(i))  //only add tuple corresponding to used slots
    			al.add(tuples[i]);
    	}
    	
        return al.iterator();
    }

}

