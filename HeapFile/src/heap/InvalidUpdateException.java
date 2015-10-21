package heap;
import chainexception.*;

public class InvalidUpdateException extends ChainException {
		public InvalidUpdateException (Exception e, String msg) {
			super(e, msg);
		}
};
