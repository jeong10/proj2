package bufmgr;
import chainexception.*;

public class PagePinnedException extends ChainException {
		public PagePinnedException (Exception e, String msg) {
			super(e, msg);
		}
};
