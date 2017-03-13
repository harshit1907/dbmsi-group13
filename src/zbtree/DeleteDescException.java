package zbtree;

import chainexception.ChainException;

public class DeleteDescException extends ChainException
{
  public DeleteDescException() {super();}
  public DeleteDescException(String s) {super(null,s);}
  public DeleteDescException(Exception e, String s) {super(e,s);}

}
