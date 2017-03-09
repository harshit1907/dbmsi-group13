package zbtree;

import chainexception.ChainException;

public class InsertDescException extends ChainException
{
  public InsertDescException() {super();}
  public InsertDescException(String s) {super(null,s);}
  public InsertDescException(Exception e, String s) {super(e,s);}

}
