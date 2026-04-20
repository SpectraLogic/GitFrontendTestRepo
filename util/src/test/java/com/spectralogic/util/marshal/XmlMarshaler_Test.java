/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;


public final class XmlMarshaler_Test 
{
    @Test
    public void testMarshalDoesSoWhenNoNestedXmlableObjectsNamingConventionCamelCaseLower()
    {
        final XmlableTestBean bean = BeanFactory.newBean( NonNestingXmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingXmlableTestBean.class ) );
        
        assertEquals(
                "<arrayProps><arrayProp>a1</arrayProp></arrayProps>"
                        + "<arrayProps><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><listProp>l1</listProp>"
                        + "<listProp>l2</listProp>"
                        + "<mySet><setProp>s1</setProp></mySet>"
                        + "<objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<stringProp/><years>33</years>",
                XmlMarshaler.marshal( bean, NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                 );
    }
    
    
    @Test
    public void testMarshalEncodesXmlCharacters()
    {
        final String unescaped = "This & string ' has \" several < xml > characters.";
        final String escaped = "This &amp; string &apos; has &quot; several &lt; xml &gt; characters.";
        final XmlableBeanWithString bean = BeanFactory.newBean( XmlableBeanWithString.class )
                .setElementWithAttribute( BeanFactory.newBean( XmlableBeanWithStringAttribute.class )
                        .setAttribute( unescaped ) )
                .setElement( unescaped );
        assertEquals(
                "<Element>" + escaped + "</Element>"
                        + "<ElementWithAttribute Attribute=\"" + escaped + "\"/>",
                XmlMarshaler.marshal( bean, NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE ),
                "Shoulda generated properly-formed XML."
                );
    }
    
    
    @Test
    public void testMarshalDoesSoWhenNoNestedXmlableObjectsNamingConventionCamelCaseUpper()
    {
        final XmlableTestBean bean = BeanFactory.newBean( NonNestingXmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingXmlableTestBean.class ) );
        
        assertEquals(
                "<ArrayProps><ArrayProp>a1</ArrayProp></ArrayProps>"
                        + "<ArrayProps><ArrayProp>a2</ArrayProp></ArrayProps>"
                        + "<BooleanProp>true</BooleanProp><ListProp>l1</ListProp>"
                        + "<ListProp>l2</ListProp>"
                        + "<MySet><SetProp>s1</SetProp></MySet>"
                        + "<ObjectBooleanProp/>"
                        + "<ObjectIntProp/><ObjectLongProp/>"
                        + "<StringProp/><Years>33</Years>",
                XmlMarshaler.marshal( bean, NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE ),
                "Shoulda generated properly-formed XML."
                );
    }
    
    
    @Test
    public void testMarshalDoesSoWhenNoNestedXmlableObjectsNamingConventionUnderscore()
    {
        final XmlableTestBean bean = BeanFactory.newBean( NonNestingXmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingXmlableTestBean.class ) );
        
        assertEquals(
                "<array_props><array_prop>a1</array_prop></array_props>"
                        + "<array_props><array_prop>a2</array_prop></array_props>"
                        + "<boolean_prop>true</boolean_prop><list_prop>l1"
                        + "</list_prop><list_prop>l2</list_prop>"
                        + "<my_set><set_prop>s1</set_prop></my_set>"
                        + "<object_boolean_prop/>"
                        + "<object_int_prop/><object_long_prop/>"
                        + "<string_prop/><years>33</years>",
                XmlMarshaler.marshal( bean, NamingConventionType.UNDERSCORED ),
                "Shoulda generated properly-formed XML."
                 );
    }
    
    
    @Test
    public void testMarshalDoesSoWhenNestedXmlableObjects()
    {
        final XmlableTestBean bean = BeanFactory.newBean( XmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( XmlableTestBean.class ) );
        
        assertEquals(
                "<arrayProps><arrayProp>a1</arrayProp></arrayProps>"
                        + "<arrayProps><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><listProp>l1</listProp>"
                        + "<listProp>l2</listProp>"
                        + "<mySet><setProp>s1</setProp></mySet><nestedBean>"
                        + "<booleanProp>false</booleanProp>"
                        + "<mySet/><nestedBean/>"
                        + "<objectBooleanProp/><objectIntProp/>"
                        + "<objectLongProp/>"
                        + "<stringProp/><years>0</years></nestedBean>"
                        + "<objectBooleanProp/><objectIntProp/>"
                        + "<objectLongProp/>"
                        + "<stringProp/><years>33</years>",
                XmlMarshaler.marshal( bean, NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                );
    }
    
    
    @Test
    public void testMarshalDoesSoWhenArrayOfXmlableObjects()
    {
        final XmlableTestBean bean = BeanFactory.newBean( NonNestingXmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingXmlableTestBean.class ) );
        
        assertEquals(
                "<nonNestingXmlableTestBean><arrayProps><arrayProp>a1</arrayProp></arrayProps>"
                        + "<arrayProps><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><listProp>l1</listProp>"
                        + "<listProp>l2</listProp>"
                        + "<mySet><setProp>s1</setProp></mySet>"
                        + "<objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<stringProp/><years>33</years></nonNestingXmlableTestBean>",
                XmlMarshaler.marshal(
                        new Object [] { bean },
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                 );
    }
    
    
    @Test
    public void testMarshalDoesSoWhenSetOfXmlableObjects()
    {
        final XmlableTestBean bean = BeanFactory.newBean( NonNestingXmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingXmlableTestBean.class ) );

        assertEquals(
                "<nonNestingXmlableTestBean><arrayProps><arrayProp>a1</arrayProp></arrayProps>"
                        + "<arrayProps><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><listProp>l1</listProp>"
                        + "<listProp>l2</listProp>"
                        + "<mySet><setProp>s1</setProp></mySet>"
                        + "<objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<stringProp/><years>33</years></nonNestingXmlableTestBean>",
                XmlMarshaler.marshal(
                        CollectionFactory.toSet( bean ),
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                 );
    }
    
    
    @Test
    public void testMarshalIncludesPropertiesThatAreMarkedAsExcludedWhenTheValueIsNullAppropriately()
    {
        final XmlableTestBean bean = BeanFactory.newBean( NonNestingXmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );

        assertEquals(
                "<nonNestingXmlableTestBean><arrayProps><arrayProp>a1</arrayProp></arrayProps>"
                        + "<arrayProps><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><listProp>l1</listProp>"
                        + "<listProp>l2</listProp>"
                        + "<mySet><setProp>s1</setProp></mySet>"
                        + "<objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<stringProp/><years>33</years></nonNestingXmlableTestBean>",
                XmlMarshaler.marshal(
                        new Object [] { bean },
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                 );
        
        bean.setListReportedWhenNotNull( new ArrayList< String >() );
        assertEquals(
                "<nonNestingXmlableTestBean><arrayProps><arrayProp>a1</arrayProp></arrayProps>"
                        + "<arrayProps><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><listProp>l1</listProp>"
                        + "<listProp>l2</listProp>"
                        + "<mySet><setProp>s1</setProp></mySet>"
                        + "<objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<stringProp/><years>33</years></nonNestingXmlableTestBean>",
                XmlMarshaler.marshal(
                        new Object [] { bean },
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                 );
        
        bean.setListReportedWhenNotNull( CollectionFactory.toList( "bob" ) );
        assertEquals(
                "<nonNestingXmlableTestBean><arrayProps><arrayProp>a1</arrayProp></arrayProps>"
                        + "<arrayProps><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><listProp>l1</listProp>"
                        + "<listProp>l2</listProp>"
                        + "<listReportedWhenNotNull>bob</listReportedWhenNotNull>"
                        + "<mySet><setProp>s1</setProp></mySet>"
                        + "<objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<stringProp/><years>33</years></nonNestingXmlableTestBean>",
                XmlMarshaler.marshal(
                        new Object [] { bean },
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                );
    }
    
    
    @Test
    public void testPerformance()
    {
        final XmlableTestBean bean = BeanFactory.newBean( XmlableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( XmlableTestBean.class ) );
        
        int count = 0;
        final Duration duration = new Duration();
        while ( 250 > duration.getElapsedMillis() )
        {
            for ( int i = 0; i < 100; ++i )
            {
                bean.setIntProp( i + count * 100 );
                XmlMarshaler.marshal( 
                        bean,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE );
            }
            count += 100;
        }
        
        final PrintStream ps = System.out;
        ps.println( "XML marshalling performance: " + (count * 4) + " marshals / second" );
    }
    
    
    @Test
    public void testPrettyFormatWithInvalidXmlNotAllowed()
    {
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test()
            {
                XmlMarshaler.formatPretty( null );
            }
        } );
        assertEquals(
                "<tag></noendtag",
                XmlMarshaler.formatPretty( "<tag></noendtag" ),
                "Shoulda returned original xml since couldn't parse xml."
                 );
    }
    
    
    @Test
    public void testPrettyFormatWithValidXmlDoesFormatXml()
    {
        assertEquals(
                "<master>" + Platform.NEWLINE + "  <tag>22</tag>" + Platform.NEWLINE
                        + "  <tag2/>" + Platform.NEWLINE + "</master>" + Platform.NEWLINE,
                XmlMarshaler.formatPretty( "<master><tag>22</tag><tag2/></master>" ),
                "Shoulda made XML pretty-formated."
                 );
    }
    
    
    private interface XmlableBeanWithString extends SimpleBeanSafeToProxy
    {
        XmlableBeanWithStringAttribute getElementWithAttribute();
        XmlableBeanWithString setElementWithAttribute( final XmlableBeanWithStringAttribute value );
        
        String getElement();
        XmlableBeanWithString setElement( final String value );
    }
    
    
    private interface XmlableBeanWithStringAttribute extends SimpleBeanSafeToProxy
    {
        @MarshalXmlAsAttribute
        String getAttribute();
        XmlableBeanWithStringAttribute setAttribute( final String value );
    }
    
    
    private interface XmlableTestBean extends TestBean, Marshalable
    {
        @ExcludeFromMarshaler( When.ALWAYS )
        long getLongProp();
        
        @CustomMarshaledName( "Years" )
        int getIntProp();
        
        @CustomMarshaledName( 
                value = "setProp", 
                collectionValue = "my_set", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        Set< String > getSetProp();
        
        @CustomMarshaledName( 
                value = "arrayProp",
                collectionValue = "arrayProps",
                collectionValueRenderingMode = CollectionNameRenderingMode.BLOCK_FOR_EVERY_ELEMENT )
        String [] getArrayProp();
        
        
        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        List< String > getListReportedWhenNotNull();
        
        void setListReportedWhenNotNull( final List< String > value );
    }
    
    
    private interface NonNestingXmlableTestBean extends XmlableTestBean
    {
        @ExcludeFromMarshaler( When.ALWAYS )
        TestBean getNestedBean();
    }
}
