/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.servlet;

import java.util.List;
import java.util.Set;



import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.Marshalable;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.testfrmwrk.TestBean;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class BeanServlet_Test
{
    @Test
    public void testXmlResponseIsCorrectWhenDataTag()
    {
        final MarshalableTestBean bean = BeanFactory.newBean( NonNestingMarshalableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingMarshalableTestBean.class ) );
        
        assertEquals(
                "<data><arrayProps><arrayProp>a1</arrayProp><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><intProp>33</intProp>"
                        + "<listProps><listProp>l1</listProp>"
                        + "<listProp>l2</listProp></listProps><objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<setProps><setProp>s1</setProp></setProps><stringProp/>"
                        + "</data>",
                new BeanServlet().getXmlResponse(
                        bean,
                        "data",
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed XML."
                 );
    }
    

    @Test
    public void testXmlResponseIsCorrectWhenNoDataTag()
    {
        final MarshalableTestBean bean = BeanFactory.newBean( NonNestingMarshalableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingMarshalableTestBean.class ) );
        
        assertEquals(
                new BeanServlet().getXmlResponse(
                        bean,
                        null,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "<arrayProps><arrayProp>a1</arrayProp><arrayProp>a2</arrayProp></arrayProps>"
                        + "<booleanProp>true</booleanProp><intProp>33</intProp>"
                        + "<listProps><listProp>l1</listProp>"
                        + "<listProp>l2</listProp></listProps><objectBooleanProp/>"
                        + "<objectIntProp/><objectLongProp/>"
                        + "<setProps><setProp>s1</setProp></setProps><stringProp/>",
                "Shoulda generated properly-formed XML."

                );
    }
    
    
    @Test
    public void testJsonResponseIsCorrectWhenDataTag()
    {
        final NonNestingMarshalableTestBean bean = BeanFactory.newBean( NonNestingMarshalableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingMarshalableTestBean.class ) );
        
        assertEquals(
                new BeanServlet().getJsonResponse(
                        bean,
                        "data",
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "{\"data\":{\"arrayProps\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33," 
                        + "\"listProps\":[\"l1\",\"l2\"],\"objectBooleanProp\":null,\"objectIntProp\":" 
                        + "null,\"objectLongProp\":null,\"setProps\":[\"s1\"],\"stringProp\":" 
                        + "null}}",
                "Shoulda generated properly-formed JSON.");
    }
    
    
    @Test
    public void testJsonResponseIsCorrectWhenNoDataTag()
    {
        final NonNestingMarshalableTestBean bean = BeanFactory.newBean( NonNestingMarshalableTestBean.class );
        bean.setBooleanProp( true );
        bean.setIntProp( 33 );
        bean.setListProp( CollectionFactory.toList( "l1", "l2" ) );
        bean.setArrayProp( new String [] { "a1", "a2" } );
        bean.setSetProp( CollectionFactory.toSet( "s1" ) );
        bean.setNestedBean( BeanFactory.newBean( NonNestingMarshalableTestBean.class ) );
        
        assertEquals(
                "{\"arrayProps\":[\"a1\",\"a2\"],\"booleanProp\":true,\"intProp\":33," 
                        + "\"listProps\":[\"l1\",\"l2\"],\"objectBooleanProp\":null,\"objectIntProp\":" 
                        + "null,\"objectLongProp\":null,\"setProps\":[\"s1\"],\"stringProp\":" 
                        + "null}",
                new BeanServlet().getJsonResponse( 
                        bean, 
                        null,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda generated properly-formed JSON.");
    }
    
    
    private interface MarshalableTestBean extends TestBean, Marshalable
    {
        @ExcludeFromMarshaler( When.ALWAYS )
        long getLongProp();
        
        @CustomMarshaledName( 
                value = "arrayProp", 
                collectionValue = "arrayProps", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        String [] getArrayProp();
        
        @CustomMarshaledName(
                value = "listProp", 
                collectionValue = "listProps",
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< String > getListProp();
        
        @CustomMarshaledName( 
                value = "setProp", 
                collectionValue = "setProps",
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        Set< String > getSetProp();
    }
    
    
    private interface NonNestingMarshalableTestBean extends MarshalableTestBean
    {
        @ExcludeFromMarshaler( When.ALWAYS )
        TestBean getNestedBean();
    }
}
