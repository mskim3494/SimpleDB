package uchidb;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


public class FilesAndExceptionsTest {

  @Test
  public void testReadandWriteFile() throws UChiDBException {
	  try {
		  //Generate test data and file
		  File t = File.createTempFile("tmptest",".txt");
		  t.deleteOnExit();
		  Random r = new Random();
		  byte[] bytes = new byte[100];
		  r.nextBytes(bytes);
		  //Write
		  FilesAndExceptions.writeFile(t, bytes);
		  
		  //Check read 1
		  int off = 0;
		  int len = 6;
		  byte[] expected = Arrays.copyOfRange(bytes, off, off+len);
		  byte[] readBytes = FilesAndExceptions.readFile(t, off, len);
		  Assert.assertArrayEquals("First check" ,expected,readBytes);
		  
		  //Check read 2
		  off = 10;
		  len = 6;
		  expected = Arrays.copyOfRange(bytes, off, off+len);
		  readBytes = FilesAndExceptions.readFile(t, off, len);
		  Assert.assertArrayEquals("Second check, not starting at offset 0", expected,readBytes);
		  
		  
	  } catch (IOException ex) {
		  ex.printStackTrace();
		  assertTrue(false);
	  }
  }
  

  @Test(expected = UChiDBException.class)
  public void testFileFail() throws UChiDBException {
	  File t = new File("DOESNOT.EXT");
	  t.deleteOnExit();
	  FilesAndExceptions.readFile(t, 10, 50);
  }
}