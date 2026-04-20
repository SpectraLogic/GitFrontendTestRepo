/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal.sax;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.xml.sax.helpers.AttributesImpl;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.marshal.sax.SaxXmlAttributeParser.AttributeIs;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class SaxXmlAttributeParser_Test 
{
    @Test
    public void testConstructorNullElementNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                    {
                        new SaxXmlAttributeParser( null, new AttributesImpl() );
                    }
             } );
    }
    

    @Test
    public void testConstructorNullAttributesNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new SaxXmlAttributeParser( "hi", null );
                }
            } );
    }
    

    @Test
    public void testGetsWorkWhenNoAttributes()
    {
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", new AttributesImpl() );
        assertNull(attributes.getLong( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getLong( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");

        attributes.verifyAllAttributesHaveBeenConsumed();
    }
    

    @Test
    public void testGetsWorkWhenLoggingEnabled()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "long", "d", "22" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        attributes.logAttributesParsed();
        assertEquals(Long.valueOf( 22 ),
                attributes.getLong( "long", AttributeIs.OPTIONAL),
                        "Should notta been any attributes.");
        assertEquals(Long.valueOf( 22 ),
                 attributes.getLong( "long", AttributeIs.REQUIRED ),
                "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");

        attributes.verifyAllAttributesHaveBeenConsumed();
    }
    

    @Test
    public void testGetsWorkWhenLongAttribute()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "long", "d", "22" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        assertEquals(Long.valueOf( 22 ),
                attributes.getLong( "long", AttributeIs.OPTIONAL ),
                "Should notta been any attributes.");
        assertEquals(Long.valueOf( 22 ),
                attributes.getLong( "long", AttributeIs.REQUIRED ),
                "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");

        attributes.verifyAllAttributesHaveBeenConsumed();
    }
    

    @Test
    public void testGetsWorkWhenStringAttribute()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "str", "d", "bye" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        assertNull(attributes.getLong( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getLong( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertEquals("bye", attributes.getString( "str", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertEquals("bye", attributes.getString( "str", AttributeIs.REQUIRED ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getEnumConstant( MockEnum.class, "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");

        attributes.verifyAllAttributesHaveBeenConsumed();
    }
    

    @Test
    public void testGetsWorkWhenEnumAttribute()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "en", "d", "smallCat" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        assertNull(attributes.getLong( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getLong( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertNull(attributes.getString( "bogus", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertEquals(MockEnum.SMALL_CAT, attributes.getEnumConstant( MockEnum.class, "en", AttributeIs.OPTIONAL ), "Should notta been any attributes.");
        assertEquals(MockEnum.SMALL_CAT, attributes.getEnumConstant( MockEnum.class, "en", AttributeIs.REQUIRED ), "Should notta been any attributes.");

        attributes.verifyAllAttributesHaveBeenConsumed();
    }
    
    
    @Test
    public void testVerifyAllAttributesHaveBeenConsumedFailsOnlyWhenThereAreRemainingAttributes()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "en", "d", "smallCat" );
        saxAttributes.addAttribute( "a", "b", "xmlns", "d", "smallCat" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                attributes.verifyAllAttributesHaveBeenConsumed();
            }
        } );
        
        attributes.getEnumConstant( MockEnum.class, "en", AttributeIs.OPTIONAL );
        attributes.verifyAllAttributesHaveBeenConsumed();
    }
    
    
    @Test
    public void testGetWhereAttributeIsRequiredAndNotPresentFails()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "en", "d", "smallCat" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        attributes.getEnumConstant( MockEnum.class, "en", AttributeIs.REQUIRED );
        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                attributes.getEnumConstant( MockEnum.class, "en2", AttributeIs.REQUIRED );
            }
        } );
        attributes.getEnumConstant( MockEnum.class, "en2", AttributeIs.OPTIONAL );
    }
    
    
    @Test
    public void testGetEnumWhereAttributeIsNotParsableFails()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "en", "d", "smallCats" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                attributes.getEnumConstant( MockEnum.class, "en", AttributeIs.OPTIONAL );
            }
        } );
    }
    
    
    @Test
    public void testGetLongWhereAttributeIsNotParsableFails()
    {
        final AttributesImpl saxAttributes = new AttributesImpl();
        saxAttributes.addAttribute( "a", "b", "en", "d", "smallCat" );
        
        final SaxXmlAttributeParser attributes = new SaxXmlAttributeParser( "hi", saxAttributes );
        TestUtil.assertThrows( null, FailureTypeObservableException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                attributes.getLong( "en", AttributeIs.OPTIONAL );
            }
        } );
    }
    
    
    private enum MockEnum
    {
        SMALL_CAT,
        BIG_CAT,
        CAT
    }
}
