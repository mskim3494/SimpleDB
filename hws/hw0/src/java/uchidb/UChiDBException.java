/**
 * 
 */
package uchidb;

/**
 * @author aelmore
 *
 */
public class UChiDBException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4659739986577468584L;

	/**
	 * 
	 */
	public UChiDBException() {
	}

	/**
	 * @param message
	 */
	public UChiDBException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public UChiDBException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public UChiDBException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public UChiDBException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
