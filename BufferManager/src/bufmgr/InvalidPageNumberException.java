package bufmgr;
import chainexception.*;

public class InvalidPageNumberException extends ChainException {
  public InvalidPageNumberException(Exception e, String name) { 
      super(e, name);
    }
}
