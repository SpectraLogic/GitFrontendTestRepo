package com.spectralogic.util.marshal.sax;

import org.xml.sax.Attributes;

public interface SaxHandler 
{
    public void handleStartElement(String elementName, Attributes  attributes);

    
    public void handleEndElement(String elementName);
}
