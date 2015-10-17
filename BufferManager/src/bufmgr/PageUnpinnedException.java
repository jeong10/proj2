package bufmgr;
import chainexception.*;

public class PageUnpinnedException extends ChainException {
		public PageUnpinnedException (Exception e, String msg) {
			super(e, msg);
		}
};
