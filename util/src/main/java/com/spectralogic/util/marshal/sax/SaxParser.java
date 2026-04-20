package com.spectralogic.util.marshal.sax;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * Just a wrapper utility for a simplified Sax Parsing Engine.
 */
public class SaxParser
{
    private final List<SaxHandler> m_handlers;
    private InputSource m_is;


    public SaxParser()
    {
        m_handlers = new LinkedList<>();
        m_is = null;
    }


    public SaxParser(SaxHandler sh)
    {
        m_handlers = new LinkedList<>();
        m_handlers.add(sh);
        m_is = null;
    }


    public void addHandler(SaxHandler shandler)
    {
        m_handlers.add(shandler);
    }


    public void setInputStream(InputStream is)
    {
        // Enruse passed in stream is parsed as a UTF-8 encoded stream: 
        final Reader r;
        try
        {
            r = new InputStreamReader( is, "UTF-8" );
        }
        catch( final UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
        final InputSource is2 = new InputSource( r );
        is2.setEncoding( "UTF-8" );
        m_is = is2;
    }


    public void parse() throws SaxException
    {
        if (m_is == null)
        {
            throw new SaxException("Input Stream not set");
        }

        final SAXParserFactory factory = SAXParserFactory.newInstance();
        try
        {
            final SAXParser saxParser = factory.newSAXParser();
            saxParser.parse(m_is, new SimpleSaxHandler());
        }
        catch (SAXException  se)
        {
            throw new SaxException("Exception parsing xml", se);
        }
        catch (ParserConfigurationException pce) 
        {
            throw new SaxException("Parser Config Exception", pce);
        } 
        catch (IOException ioe) 
        {
            throw new SaxException("IO exception parsing xml", ioe);
        }
        // InputStream is not closes as it is passed into this class
    }


    private class SimpleSaxHandler extends DefaultHandler
    {
        @Override
        public void startElement(
                String uri, String localName, String qname, Attributes atts) throws SAXException
        {
            if (m_handlers == null || m_handlers.size() == 0)
            {
                return;
            }

            for (SaxHandler  ish: m_handlers)
            {
                ish.handleStartElement(qname, atts);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qname) throws SAXException
        {
            if (m_handlers == null || m_handlers.size() == 0)
            {
                return;
            }

            for (SaxHandler  ish: m_handlers)
            {
                ish.handleEndElement(qname);
            }
        }

        @Override
        public void characters(char[] ch, int  start, int length) throws SAXException
        {
            if (m_handlers == null || m_handlers.size() == 0)
            {
                return;
            }

            for (SaxHandler  ish: m_handlers)
            {
                if (ish instanceof SaxDataHandler)
                {
                    SaxDataHandler isdh = (SaxDataHandler) ish;
                    isdh.handleBodyData(ch, start, length);
                }
            }
        }
    } // end inner class def
}
