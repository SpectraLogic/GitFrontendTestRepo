/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import com.spectralogic.util.lang.NamingConventionType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class BaseMarshalable_Test 
{
    @Test
    public void testToJsonRetunsSerializedBean()
    {
        final String expectedSerializedInnerMarshalable =
                "{\"IntField\":20,\"MarshalableTypeField\":null,\"StringField\":\"bar\"}";
        assertEquals(
                "{\"IntField\":10,\"MarshalableTypeField\":"
                        + expectedSerializedInnerMarshalable
                        + ",\"StringField\":\"foo\"}",
                buildMarshalableTypeValue().toJson(),
                "Shoulda returned the expected JSON string."
                 );
    }
    
    
    @Test
    public void testToXmlRetunsSerializedBean()
    {
        final String expectedSerializedInnerMarshalable =
                "<IntField>20</IntField><MarshalableTypeField/><StringField>bar</StringField>";
        assertEquals(
                "<IntField>10</IntField><MarshalableTypeField>"
                        + expectedSerializedInnerMarshalable
                        + "</MarshalableTypeField><StringField>foo</StringField>",
                buildMarshalableTypeValue().toXml(),
                "Shoulda returned the expected XML string."
                 );
    }
    
    
    @Test
    public void testToXmlRetunsSerializedBeanWhenNamingConventionIsCamelLower()
    {
        final String expectedSerializedInnerMarshalable =
                "<intField>20</intField><marshalableTypeField/><stringField>bar</stringField>";
        assertEquals(
                "<intField>10</intField><marshalableTypeField>"
                        + expectedSerializedInnerMarshalable
                        + "</marshalableTypeField><stringField>foo</stringField>",
                buildMarshalableTypeValue().toXml(
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ),
                "Shoulda returned the expected XML string."
                 );
    }
    
    
    @Test
    public void testToXmlRetunsSerializedBeanWhenNamingConventionIsConcatLowercase()
    {
        final String expectedSerializedInnerMarshalable =
                "<intfield>20</intfield><marshalabletypefield/><stringfield>bar</stringfield>";
        assertEquals(
                "<intfield>10</intfield><marshalabletypefield>"
                        + expectedSerializedInnerMarshalable
                        + "</marshalabletypefield><stringfield>foo</stringfield>",
                buildMarshalableTypeValue().toXml( NamingConventionType.CONCAT_LOWERCASE ),
                "Shoulda returned the expected XML string."
                 );
    }
    
    
    @Test
    public void testToXmlRetunsSerializedBeanWhenNamingConventionIsConstant()
    {
        final String expectedSerializedInnerMarshalable =
                "<INT_FIELD>20</INT_FIELD><MARSHALABLE_TYPE_FIELD/><STRING_FIELD>bar</STRING_FIELD>";
        assertEquals(
                "<INT_FIELD>10</INT_FIELD><MARSHALABLE_TYPE_FIELD>"
                        + expectedSerializedInnerMarshalable
                        + "</MARSHALABLE_TYPE_FIELD><STRING_FIELD>foo</STRING_FIELD>",
                buildMarshalableTypeValue().toXml( NamingConventionType.CONSTANT ),
                "Shoulda returned the expected XML string."
                 );
    }
    
    
    @Test
    public void testToXmlRetunsSerializedBeanWhenNamingConventionIsUnderscored()
    {
        final String expectedSerializedInnerMarshalable =
                "<int_field>20</int_field><marshalable_type_field/><string_field>bar</string_field>";
        assertEquals(
                "<int_field>10</int_field><marshalable_type_field>"
                        + expectedSerializedInnerMarshalable
                        + "</marshalable_type_field><string_field>foo</string_field>",
                buildMarshalableTypeValue().toXml( NamingConventionType.UNDERSCORED ),
                "Shoulda returned the expected XML string."
                 );
    }
    
    
    @Test
    public void testToStringDoesNotBlowUp()
    {
        assertNotNull(
                buildMarshalableTypeValue().toString(),
                "Shoulda returned a toString() value."
                );
    }


    private static MarshalableType buildMarshalableTypeValue()
    {
        final MarshalableType innerMarshalableType = new MarshalableType();
        innerMarshalableType.setIntField( 20 );
        innerMarshalableType.setStringField( "bar" );
        
        final MarshalableType marshalableType = new MarshalableType();
        marshalableType.setIntField( 10 );
        marshalableType.setStringField( "foo" );
        marshalableType.setMarshalableTypeField( innerMarshalableType );
        return marshalableType;
    }
    
    
    public static final class MarshalableType extends BaseMarshalable
    {
        public String getStringField()
        {
            return m_stringField;
        }
        
        
        public void setStringField( final String stringField )
        {
            m_stringField = stringField;
        }
        
        
        public int getIntField()
        {
            return m_intField;
        }
        
        
        public void setIntField( final int intField )
        {
            m_intField = intField;
        }
        
        
        public MarshalableType getMarshalableTypeField()
        {
            return m_marshalableTypeField;
        }


        public void setMarshalableTypeField( final MarshalableType marshalableTypeField )
        {
            m_marshalableTypeField = marshalableTypeField;
        }


        private String m_stringField;
        private int m_intField;
        private MarshalableType m_marshalableTypeField;
    }// end inner class
}
