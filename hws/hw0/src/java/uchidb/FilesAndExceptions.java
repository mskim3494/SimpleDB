package uchidb;

//import java.io.ByteArrayOutputStream; //newly added -> maybe not needed
import java.io.File;
import java.io.FileInputStream; //newly added by me
import java.io.FileOutputStream; //newly added
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.io.FileNotFoundException;  already included as part of IOException

public class FilesAndExceptions {


	/**
	 * Read a number of bytes from a file. Throw an exception is the read fails. DO NOT CLOSE THE FILE
	 * @param file The file to read from 
	 * @param offset byte position to read from 
	 * @param len number of bytes to read from the file
	 * @return byte[] of the bytes from the file
	 * @throws UChiDBException if file is not found, or if range is not valid
	 */
	public static byte[] readFile(File file, int offset, int len) throws UChiDBException {
		InputStream is = null;
		byte[] bytes = null;
		try {
			//nullpointer: if bytes = null. but it is null! so setting by len
			
			

			//set up a fileinputstream and read based on parameters.
			is = new FileInputStream(file);
			//System.out.println("available bytes to read: " + is.available());
			//bytes = new byte[is.available()];  //this should be the actual length of the file!
			bytes = new byte[len];  //no.. len bytes are expected
			//System.out.println("len: " + len + " b.length - off: " + bytes.length + " - " + offset);
			
			//new try, since offset for is.read() is not for the file but for the buffer
			is.skip(offset);
			is.read(bytes, 0, len); //works!
			
			//is.read(bytes, offset, len); //only read the specified range and write to bytes
			//System.out.println("read bytes: " + bytes);
			// returns total num of bytes read, -1 if end reached
			//IO, nullpointer, indexoutofbounds exceptions
			

			
			//TODO directly read the bytes from the file. do not read the entire file			 
		} catch (IOException | NullPointerException | IndexOutOfBoundsException ex) {
			//TODO handle the file exceptions and throw appropriate exception
			//You may need to add additional exceptions
			
			//System.out.println(ex);
			throw new UChiDBException(ex.getMessage());
			
			//https://www.journaldev.com/629/java-catch-multiple-exceptions-rethrow-exception
			//UChiDBException has does have a constructor with String arg
			
		} finally {
			//finally will always execute, regardless if an exception is thrown
			//clean up resources here
			
			//TODO close is
			try {
				is.close(); //doc says this releases any related system resources
				//so I guess this deals with cleaning up?
			}
			catch (IOException | NullPointerException | IndexOutOfBoundsException ex) {
				//System.out.println(ex);
				throw new UChiDBException(ex.getMessage());
			} //if I don't add this error says unhandled exception type IOException
			
			
		}
		return bytes;	//the read output is returned here
	}



	/** Write bytes to a file. DO NOT CLOSE THE FILE. 
	 * @param file
	 * @param bytes
	 * @throws UChiDBException If write fails
	 */
	public static void writeFile(File file, byte[] bytes) throws UChiDBException {
		OutputStream os = null;
		try {
			//TODO Write the file
			
			os = new FileOutputStream(file);
			os.write(bytes); //there's another version with offset & length,
			//but maybe this should be the one? cuz we don't specify those params
			//only throws IOException when file fails to open
			
			//other edge cases:
			//1) what if file length > bytes length?
			//2) what if bytes length > file length?
			//not sure how write would act in these cases...
			
		} catch (IOException e) {
			//TODO handle the file exceptions and throw appropriate exception
			//You may need to add additional exceptions
			e.printStackTrace();
			
			throw new UChiDBException(e.getMessage()); //e here instead of ex
			
		} finally {
			//finally will always execute, regardless if an exception is thrown
			//clean up resources here
			//TODO close the os     
			
			//try the same try-catch as in is.close()
			try {
				os.close(); //again, this releases related resources 
			}
			catch (IOException e) {
				throw new UChiDBException(e.getMessage());
			} 
			
		}
	}

}
