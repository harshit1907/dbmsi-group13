package zbtree;

import chainexception.ChainException;

public class DescriptorNotMatchException extends ChainException
{
  public DescriptorNotMatchException() {super();}
  public DescriptorNotMatchException(String s) {super(null,s);}
  public DescriptorNotMatchException(Exception e, String s) {super(e,s);}

}
