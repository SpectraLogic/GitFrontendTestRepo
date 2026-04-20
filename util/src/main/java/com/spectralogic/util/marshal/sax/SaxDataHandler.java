package com.spectralogic.util.marshal.sax;

public interface SaxDataHandler extends SaxHandler
{
    public void handleBodyData(char [] data, int start, int length);
}
