package bufmgr;
import chainexception.*;

public class BufferPoolExceededException extends ChainException {
		public BufferPoolExceededException (ChainException e, String msg) {
			super(e, msg);
		}
};
