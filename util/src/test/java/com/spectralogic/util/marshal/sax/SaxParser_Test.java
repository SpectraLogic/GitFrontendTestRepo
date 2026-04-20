/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal.sax;

import java.lang.reflect.Field;
import java.nio.charset.Charset;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.io.IOUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class SaxParser_Test 
{

    @Test
    public void testParseWithNullInputStreamIsNotAllowed()
    {
        TestUtil.assertThrows( null, SaxException.class, new BlastContainer()
        {
            @Override
            public void test() throws SaxException
            {
                new SaxParser().parse();
            }
        } );
    }
    
    
    @Test
    public void testParseWithInvalidXmlIsNotAllowed()
    {
        final SaxParser parser = new SaxParser();
        parser.setInputStream( IOUtils.toInputStream( INVALID_TEST_DOCUMENT, Charset.defaultCharset() ) );
        
        TestUtil.assertThrows( null, SaxException.class, new BlastContainer()
        {
            @Override

            public void test() throws SaxException
            {
                parser.parse();
            }
        } );
    }
    

    @Test
    public void testParseWithEmptyConstructor() throws SaxException
    {
        final StubSaxHandler handler = new StubSaxHandler();

        final SaxParser parser = new SaxParser();
        parser.setInputStream( IOUtils.toInputStream( TEST_DOCUMENT, Charset.defaultCharset() ) );
        parser.addHandler( handler );
        parser.parse();

        assertEquals(2,  handler.getNodeCount(), "Shoulda traversed the correct number of <node>s.");
    }

    
    @Test
    public void testParseWithNonEmptyConstructor() throws SaxException
    {
        final StubSaxHandler handler = new StubSaxHandler();

        final SaxParser parser = new SaxParser( handler );
        parser.setInputStream( IOUtils.toInputStream( TEST_DOCUMENT, Charset.defaultCharset() ) );
        parser.parse();

        assertEquals(2,  handler.getNodeCount(), "Shoulda traversed the correct number of <node>s.");
    }

    
    @Test
    public void testParseWithDataHandler() throws SaxException
    {
        final StubSaxDataHandler handler = new StubSaxDataHandler();

        final SaxParser parser = new SaxParser( handler );
        parser.setInputStream( IOUtils.toInputStream( TEST_DOCUMENT, Charset.defaultCharset() ) );
        parser.parse();

        assertEquals("fooo", handler.getCharacters(), "Shoulda had the right raw character data.");
    }
    
    
    @Test
    public void testEnsurePassedInStreamIsInterpretedAsUTF8()
    {
        final StubSaxDataHandler handler = new StubSaxDataHandler();

        final SaxParser parser = new SaxParser( handler );
        parser.setInputStream( IOUtils.toInputStream( TEST_DOCUMENT,
                                                      Charset.defaultCharset() ) );
        final Field f;
        try
        {
            f = SaxParser.class.getDeclaredField( "m_is" );
        }
        catch ( final NoSuchFieldException | SecurityException ex )
        {
            throw new RuntimeException( ex );
        }
        f.setAccessible( true );
        try
        {
            assertEquals("UTF-8", ((InputSource)f.get( parser )).getEncoding(), "UTF-8 shoulda' been the encoding on the InputSource.");
        }
        catch ( final IllegalArgumentException | IllegalAccessException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    private final class StubSaxHandler implements SaxHandler
    {
        private int m_nodeCount = 0;
        
        public int getNodeCount()
        {
            return m_nodeCount;
        }
        
        @Override
        public void handleStartElement( String elementName, Attributes attributes )
        {
            switch (elementName)
            {
                case "root":
                    break;
                case "node":
                    assertEquals(1,  attributes.getLength(), "Shoulda had the correct number of XML attributes.");
                    assertEquals("id", attributes.getLocalName( 0 ), "Shoulda had the correct attribute name.");
                    assertEquals(Integer.toString( m_nodeCount++ ), attributes.getValue( 0 ), "Shoulda had the correct id attribute value.");
                    break;
                default:
                    throw new RuntimeException( String.format( "Unexpected node '%s'", elementName) );
            }
        }

        @Override
        public void handleEndElement( String elementName )
        {
            // Do nothing
        }
    }// end inner class
    
    
    private final class StubSaxDataHandler implements SaxDataHandler
    {
        private boolean m_inNode = false;
        private final StringBuilder m_characters = new StringBuilder();
        
        public String getCharacters()
        {
            return m_characters.toString();
        }

        public void handleStartElement( String elementName, Attributes attributes )
        {
            if ( elementName.equals( "node" ) )
            {
                m_inNode = true;
            }
        }

        public void handleEndElement( String elementName )
        {
            if ( elementName.equals( "node" ) )
            {
                m_inNode = false;
            }
        }

        public void handleBodyData( char[] data, int start, int length )
        {
            assertTrue(
                    m_inNode,
                    "Shoulda been inside of a <node> element." );
            
            m_characters.append( data, start, length );
        }
    }// end inner class
    
    
    private static final String TEST_DOCUMENT = "<root><node id='0' /><node id='1'>fooo</node></root>";
    private static final String INVALID_TEST_DOCUMENT = "<root<<<<><node id='0' /><node id='1' /></root>";
}
