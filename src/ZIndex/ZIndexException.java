package ZIndex;

import chainexception.ChainException;

public class ZIndexException extends ChainException
{
  public ZIndexException() {super();}
  public ZIndexException(String s) {super(null,s);}
  public ZIndexException(Exception e, String s) {super(e,s);}
}
