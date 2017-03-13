package zbtree;

import chainexception.ChainException;

public class IndexInsertDescException extends ChainException
{
  public IndexInsertDescException() {super();}
  public IndexInsertDescException(String s) {super(null,s);}
  public IndexInsertDescException(Exception e, String s) {super(e,s);}

}
