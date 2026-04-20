/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class JsonMarshaler_Test 
{
    @Test
    public void testJsonEncodeNullStringReturnsNull()
    {
        assertNull(
                JsonMarshaler.jsonEncode( null ),
                "Null input shoulda resulted in null output."
                );
    }
    
    
    @Test
    public void testJsonEncodeNonNullStringReturnsNonNull()
    {
        assertNotNull(
                JsonMarshaler.jsonEncode( "string" ),
                "Null input shoulda resulted in null output."
                 );
    }
    
    
    @Test
    public void testJsonEncodeReturnsProperlyFormattedJson()
    {
        assertEquals("hello bob!",
                JsonMarshaler.jsonEncode(
                  "hello bob!" ),
                "Shoulda formatted String in correct JSON format.");
        assertEquals("\\b",
                JsonMarshaler.jsonEncode(
                  "\b" ),
                "Shoulda formatted String in correct JSON format.");
        assertEquals("\\b\\\\",
                JsonMarshaler.jsonEncode(
                  "\b\\" ),
                "Shoulda formatted String in correct JSON format.");
        assertEquals("hello bob \\n \\\\ \\\" \\/",
                JsonMarshaler.jsonEncode(
                  "hello bob \n \\ \" /" ),
                "Shoulda formatted String in correct JSON format.");
        assertEquals("hello bob \\b \\f\\r\\t\\t",
                JsonMarshaler.jsonEncode(
                  "hello bob \b \f\r\t\t" ),
                "Shoulda formatted String in correct JSON format.");
    }
    
    
    @Test
    public void testMarshalDoesSoWhenNoNestedJsonableObjects()
    {
        final JsonableTestBean bean = BeanFactory.newBean( NonNestingJsonableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingJsonableTestBean.class ) );

        assertEquals("{\"arrayProp\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33,"
                        + "\"listProp\":[\"l1\",\"l2\"],\"objectBooleanProp\":null,\"objectIntProp\":"
                        + "null,\"objectLongProp\":null,\"setProp\":[\"s1\"],\"stringProp\":"
                        + "null}", JsonMarshaler.marshal(
                        bean,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ), "Shoulda generated properly-formed JSON.");
    }
    
    
    @Test
    public void testMarshalDoesSoWhenNestedJsonableObjects()
    {
        final JsonableTestBean bean = BeanFactory.newBean( JsonableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( JsonableTestBean.class ) );

        assertEquals("{\"arrayProp\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33,"
                        + "\"listProp\":[\"l1\",\"l2\"],\"nestedBean\":{"
                        + "\"arrayProp\":[],\"booleanProp\":false,\"intProp\":0,"
                        + "\"listProp\":[],\"nestedBean\":null,"
                        + "\"objectBooleanProp\":null,\"objectIntProp\":null,"
                        + "\"objectLongProp\":null,\"setProp\":[],\"stringProp\":null}"
                        + ",\"objectBooleanProp\":null,\"objectIntProp\":"
                        + "null,\"objectLongProp\":null,\"setProp\":[\"s1\"],\"stringProp\":"
                        + "null}", JsonMarshaler.marshal(
                        bean,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ), "Shoulda generated properly-formed JSON.");

    }
    
    
    @Test
    public void testMarshalDoesSoWhenArrayOfJsonableObjects()
    {
        final JsonableTestBean bean = BeanFactory.newBean( NonNestingJsonableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingJsonableTestBean.class ) );

        assertEquals("["
                        + "{\"arrayProp\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33,"
                        + "\"listProp\":[\"l1\",\"l2\"],\"objectBooleanProp\":null,\"objectIntProp\":"
                        + "null,\"objectLongProp\":null,\"setProp\":[\"s1\"],\"stringProp\":"
                        + "null}"
                        + "]", JsonMarshaler.marshal(
                        new Object [] { bean },
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ), "Shoulda generated properly-formed JSON.");
    }
    
    
    @Test
    public void testMarshalDoesSoWhenSetOfJsonableObjects()
    {
        final JsonableTestBean bean = BeanFactory.newBean( NonNestingJsonableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingJsonableTestBean.class ) );

        assertEquals("["
                        + "{\"arrayProp\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33,"
                        + "\"listProp\":[\"l1\",\"l2\"],\"objectBooleanProp\":null,\"objectIntProp\":"
                        + "null,\"objectLongProp\":null,\"setProp\":[\"s1\"],\"stringProp\":"
                        + "null}"
                        + "]", JsonMarshaler.marshal(
                        CollectionFactory.toSet( bean ),
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ), "Shoulda generated properly-formed JSON.");
    }
    
    
    @Test
    public void testPerformance()
    {
        final JsonableTestBean bean = BeanFactory.newBean( JsonableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( JsonableTestBean.class ) );
        
        int count = 0;
        final Duration duration = new Duration();
        while ( 250 > duration.getElapsedMillis() )
        {
            for ( int i = 0; i < 100; ++i )
            {
                bean.setIntProp( i + count * 100 );
                JsonMarshaler.marshal(
                        bean,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE );
            }
            count += 100;
        }
        
        final PrintStream ps = System.out;
        ps.println( "JSON marshalling performance: " + (count * 4) + " marshals / second" );
    }
    
    
    @Test
    public void testPrettyFormatWithInvalidJsonReturnsInvalidJson()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
                {
                    JsonMarshaler.formatPretty( null );
                }
            } );
        assertEquals("{\"master\":value", JsonMarshaler.formatPretty( "{\"master\":value" ), "Shoulda returned original json since can't parse json.");
    }
    
    
    @Test
    public void testPrettyFormatWithValidJsonDoesFormatJson()
    {
        assertEquals("{\"master\": {" + Platform.SLASH_N +
                "  \"tag\": \"22\"," + Platform.SLASH_N +
                "  \"tag2\": \"33\"" + Platform.SLASH_N +
                "}}", JsonMarshaler.formatPretty( "{\"master\":{\"tag\":\"22\",\"tag2\":\"33\"}}" ), "Shoulda made JSON pretty-formated.");

        assertNotNull(
                "Shoulda formatted valid JSON.",
                JsonMarshaler.formatPretty( 
"{\"ListBucketResult\":{\"Prefix\":[],\"Contents\":[],\"CreationDate\":\"2015-10-22T21:12:45.000Z\"," 
+ "\"Delimiter\":null,\"IsTruncated\":false,\"Marker\":null,\"MaxKeys\":1000,\"Name\":\"test\"," 
+ "\"NextMarker\":null}}" ) );
        assertNotNull(
                "Shoulda formatted valid JSON.",
                JsonMarshaler.formatPretty( 
"[{\"ListBucketResult\":{\"Prefix\":[],\"Contents\":[],\"CreationDate\":\"2015-10-22T21:12:45.000Z\"," 
+ "\"Delimiter\":null,\"IsTruncated\":false,\"Marker\":null,\"MaxKeys\":1000,\"Name\":\"test\"," 
+ "\"NextMarker\":null}}]" ) );
    }
    
    
    @Test
    public void testMarshalIncludesPropertiesThatAreMarkedAsExcludedWhenTheValueIsNullAppropriately()
    {
        final JsonableTestBean bean = BeanFactory.newBean( NonNestingJsonableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingJsonableTestBean.class ) );

        assertEquals("{\"arrayProp\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33,"
                        + "\"listProp\":[\"l1\",\"l2\"],\"objectBooleanProp\":null,\"objectIntProp\":"
                        + "null,\"objectLongProp\":null,\"setProp\":[\"s1\"],\"stringProp\":"
                        + "null}", JsonMarshaler.marshal(
                        bean,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ), "Shoulda generated properly-formed JSON.");

        bean.setListReportedWhenNotNull( new ArrayList< String >() );
        assertEquals("{\"arrayProp\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33,"
                        + "\"listProp\":[\"l1\",\"l2\"],\"objectBooleanProp\":null,\"objectIntProp\":"
                        + "null,\"objectLongProp\":null,\"setProp\":[\"s1\"],\"stringProp\":"
                        + "null}", JsonMarshaler.marshal(
                        bean,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ), "Shoulda generated properly-formed JSON.");

        bean.setListReportedWhenNotNull( CollectionFactory.toList( "bob" ) );
        assertEquals("{\"arrayProp\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33,"
                        + "\"listProp\":[\"l1\",\"l2\"],"
                        + "\"listReportedWhenNotNull\":[\"bob\"],"
                        + "\"objectBooleanProp\":null,\"objectIntProp\":"
                        + "null,\"objectLongProp\":null,\"setProp\":[\"s1\"],\"stringProp\":"
                        + "null}", JsonMarshaler.marshal(
                        bean,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ), "Shoulda generated properly-formed JSON.");
    }
    
    
    @Test
    public void testUnmarshallingOfSerializedBeansWorksRegardlessOfJsonFormattingAndNamingConvention()
    {
        final UnmarshalableBean bean = BeanFactory.newBean( UnmarshalableBean.class );
        assertUnmarshalWorks( bean );
        
        bean.setIntProp( 33 );
        assertUnmarshalWorks( bean );
        bean.setIds( new UUID [] { UUID.randomUUID(), UUID.randomUUID() } );
        assertUnmarshalWorks( bean );
        bean.setNestedBean( BeanFactory.newBean( UnmarshalableBean.class ) );
        assertUnmarshalWorks( bean );
        bean.getNestedBean().setStringProp( "nested" );
        assertUnmarshalWorks( bean );
        bean.setNestedBeans( 
                new UnmarshalableBean [] { null, BeanFactory.newBean( UnmarshalableBean.class ) } );
        assertUnmarshalWorks( bean );
        bean.getNestedBeans()[ 1 ].setStringProp( "arrayNested" );
        assertUnmarshalWorks( bean );
        bean.getNestedBeans()[ 1 ].setNestedBean( BeanFactory.newBean( UnmarshalableBean.class ) );
        bean.getNestedBeans()[ 1 ].getNestedBean().setNestedBeans( 
                new UnmarshalableBean [] { BeanFactory.newBean( UnmarshalableBean.class ) } );
        bean.getNestedBeans()[ 1 ].getNestedBean().getNestedBeans()[ 0 ].setStringProp( "nesteeeeed" );
        assertUnmarshalWorks( bean );
    }
    
    
    @Test
    public void testUnmarshallingOfSerializedBeansWorksWhenPayloadContainsSpecialCharacters()
    {
        final UnmarshalableBean bean = BeanFactory.newBean( UnmarshalableBean.class );
        assertUnmarshalWorks( bean );
        
        final RuntimeException ex;
        try
        {
            try
            {
                throw new RuntimeException( "Hello." );
            }
            catch ( final RuntimeException ex2 )
            {
                throw new RuntimeException( "Oops.", ex2 );
            }
        }
        catch ( final RuntimeException ex2 )
        {
            Validations.verifyNotNull( "Shut up CodePro", ex2 );
            ex = ex2;
        }
        
        bean.setIntProp( 33 );
        assertUnmarshalWorks( bean );
        bean.setIds( new UUID [] { UUID.randomUUID(), UUID.randomUUID() } );
        assertUnmarshalWorks( bean );
        bean.setNestedBean( BeanFactory.newBean( UnmarshalableBean.class ) );
        assertUnmarshalWorks( bean );
        bean.getNestedBean().setStringProp(
                ExceptionUtil.getFullMessage( ex ) + " - " + ExceptionUtil.getFullStackTrace( ex ) );
        assertUnmarshalWorks( bean );
        bean.setNestedBeans( 
                new UnmarshalableBean [] { null, BeanFactory.newBean( UnmarshalableBean.class ) } );
        assertUnmarshalWorks( bean );
        bean.getNestedBeans()[ 1 ].setStringProp( "arrayNested" );
        assertUnmarshalWorks( bean );
        bean.getNestedBeans()[ 1 ].setNestedBean( BeanFactory.newBean( UnmarshalableBean.class ) );
        bean.getNestedBeans()[ 1 ].getNestedBean().setNestedBeans( 
                new UnmarshalableBean [] { BeanFactory.newBean( UnmarshalableBean.class ) } );
        bean.getNestedBeans()[ 1 ].getNestedBean().getNestedBeans()[ 0 ].setStringProp( "nesteeeeed" );
        assertUnmarshalWorks( bean );
    }
    
    
    @Test
    public void testMarshalingAndUnmarshalingOfDatesWork()
    {
        final Date date = new Date();
        final UnmarshalableBean bean = BeanFactory.newBean( UnmarshalableBean.class );
        bean.setDateProp( date );
        assertTrue(
                bean.toJson( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ).contains(
                        "\"customDate\":" + date.getTime() ),
                "Shoulda marshaled date correctly."
                 );
        assertUnmarshalWorks( bean );
    }
    
    
    private void assertUnmarshalWorks( final UnmarshalableBean bean )
    {
        for ( final NamingConventionType nc : NamingConventionType.values() )
        {
            if ( NamingConventionType.CONCAT_LOWERCASE == nc )
            {
                continue;
            }
            
            assertUnmarshalWorked(
                    bean,
                    JsonMarshaler.unmarshal( UnmarshalableBean.class, JsonMarshaler.marshal( bean, nc ) ) );
            assertUnmarshalWorked(
                    bean,
                    JsonMarshaler.unmarshal( UnmarshalableBean.class, JsonMarshaler.formatPretty( 
                            JsonMarshaler.marshal( bean, nc ) ) ) );
        }
    }
    
    
    private void assertUnmarshalWorked(
            final UnmarshalableBean marshaledBean,
            final UnmarshalableBean unmarshaledBean )
    {
        if ( null == marshaledBean && null == unmarshaledBean )
        {
            return;
        }
        if ( null == marshaledBean || null == unmarshaledBean )
        {
            throw new RuntimeException( "If both beans aren't null, neither should be null." );
        }

        assertEquals(marshaledBean.getIntProp(), unmarshaledBean.getIntProp(), "Shoulda unmarshaled properly.");
        assertEquals(marshaledBean.getLongProp(), unmarshaledBean.getLongProp(), "Shoulda unmarshaled properly.");
        assertEquals(marshaledBean.getDateProp(), unmarshaledBean.getDateProp(), "Shoulda unmarshaled properly.");
        final UUID [] originalIds = ( null == marshaledBean.getIds() ) ?
                (UUID [])Array.newInstance( UUID.class, 0 )
                : marshaledBean.getIds();
        assertTrue(
                Arrays.deepEquals( originalIds, unmarshaledBean.getIds() ),
                "Shoulda unmarshaled properly."
                 );
        final UnmarshalableBean [] originalNestedBeans = ( null == marshaledBean.getNestedBeans() ) ?
                (UnmarshalableBean[])Array.newInstance( UnmarshalableBean.class, 0 )
                : marshaledBean.getNestedBeans();
        assertEquals(originalNestedBeans.length,  unmarshaledBean.getNestedBeans().length, "Shoulda unmarshaled properly.");
        for ( int i = 0; i < originalNestedBeans.length; ++i )
        {
            assertUnmarshalWorked(
                    originalNestedBeans[ i ], 
                    unmarshaledBean.getNestedBeans()[ i ] );
        }
    }
    
    
    private interface UnmarshalableBean extends SimpleBeanSafeToProxy
    {
        UnmarshalableBean getNestedBean();
        
        void setNestedBean( final UnmarshalableBean value );
        
        
        int getIntProp();
        
        void setIntProp( final int value );
        
        
        Long getLongProp();
        
        void setLongProp( final Long value );
        

        @CustomMarshaledName( "CustomDate" )
        Date getDateProp();
        
        void setDateProp( final Date value );
        
        
        String getStringProp();
        
        void setStringProp( final String value );
        
        
        @CustomMarshaledName( 
                collectionValue = "CustomIds", 
                value = "Id",
                collectionValueRenderingMode = CollectionNameRenderingMode.BLOCK_FOR_EVERY_ELEMENT )
        UUID [] getIds();
        
        void setIds( final UUID [] value );
        
        
        UnmarshalableBean [] getNestedBeans();
        
        void setNestedBeans( final UnmarshalableBean [] value );
    }
    
    
    private interface JsonableTestBean extends TestBean, Marshalable
    {
        @ExcludeFromMarshaler( When.ALWAYS )
        long getLongProp();
        
        
        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        List< String > getListReportedWhenNotNull();
        
        void setListReportedWhenNotNull( final List< String > value );
    }
    
    
    private interface NonNestingJsonableTestBean extends JsonableTestBean
    {
        @ExcludeFromMarshaler( When.ALWAYS )
        TestBean getNestedBean();
    }
}
