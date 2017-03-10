package zbtree;

import chainexception.ChainException;

public class LeafInsertDescException extends ChainException
{
  public LeafInsertDescException() {super();}
  public LeafInsertDescException(String s) {super(null,s);}
  public LeafInsertDescException(Exception e, String s) {super(e,s);}

}
