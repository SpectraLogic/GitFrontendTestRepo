package com.spectralogic.util.marshal.sax;

public class SaxException extends Exception
{
    public SaxException(String message)
    {
        super(message);
    }


    public SaxException(String message, Exception e)
    {
        super(message, e);
    }
}
