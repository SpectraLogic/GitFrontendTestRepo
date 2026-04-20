/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class NamingConvention_Test 
{
    @Test
    public void testToConstantNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    NamingConvention.toConstantNamingConvention( null );
                }
            } );
    }
    
    
    @Test
    public void testToConstantDoesSo()
    {
        assertEquals("NAME_PROP", NamingConvention.toConstantNamingConvention( "NAME_PROP" ), "Shoulda converted correctly.");
        assertEquals("NAME_PROP", NamingConvention.toConstantNamingConvention( "name_prop" ), "Shoulda converted correctly.");
        assertEquals("S3_ID_PROP", NamingConvention.toConstantNamingConvention( "s3_id_prop" ), "Shoulda converted correctly.");
        assertEquals("S3_ID_PROP", NamingConvention.toConstantNamingConvention( "s3-id-prop" ), "Shoulda converted correctly.");
        assertEquals("S3_ID_PROP", NamingConvention.toConstantNamingConvention( "s3IdProp" ), "Shoulda converted correctly.");
    }
    
    
    @Test
    public void testToConcatenatedLowercaseNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                NamingConvention.toConcatenatedLowercase( null );
            }
        } );
    }
    
    
    @Test
    public void testToConcatenatedLowercaseDoesSo()
    {
        assertEquals("nameprop", NamingConvention.toConcatenatedLowercase( "NAME_PROP" ), "Shoulda converted correctly.");
        assertEquals("nameprop", NamingConvention.toConcatenatedLowercase( "name_prop" ), "Shoulda converted correctly.");
        assertEquals("s3idprop", NamingConvention.toConcatenatedLowercase( "s3_id_prop" ), "Shoulda converted correctly.");
        assertEquals("s3idprop", NamingConvention.toConcatenatedLowercase( "s3-id-prop" ), "Shoulda converted correctly.");
        assertEquals("s3idprop", NamingConvention.toConcatenatedLowercase( "s3IdProp" ), "Shoulda converted correctly.");
    }
    
    
    @Test
    public void testToCamelCaseNamingConventionWithFirstLetterLowercaseNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( null );
            }
        } );
    }
    
    
    @Test
    public void testToCamelCaseNamingConventionWithFirstLetterLowercaseDoesSo()
    {
        assertEquals("nameProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( "NAME_PROP" ), "Shoulda converted correctly.");
        assertEquals("nameProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( "name_prop" ), "Shoulda converted correctly.");
        assertEquals("s3IdProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( "s3_id_prop" ), "Shoulda converted correctly.");
        assertEquals("s3IdProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( "s3-id-prop" ), "Shoulda converted correctly.");
        assertEquals("s3IdProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( "s3IdProp" ), "Shoulda converted correctly.");
        assertEquals("someProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( "SomeProp" ), "Shoulda converted correctly.");
    }
    

    @Test
    public void testToCamelCaseNamingConventionWithFirstLetterUppercaseNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( null );
            }
        } );
    }
    
    
    @Test
    public void testToCamelCaseNamingConventionWithFirstLetterUppercaseDoesSo()
    {
        assertEquals("NameProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterUppercase( "NAME_PROP" ), "Shoulda converted correctly.");
        assertEquals("NameProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterUppercase( "name_prop" ), "Shoulda converted correctly.");
        assertEquals("S3IdProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterUppercase( "s3_id_prop" ), "Shoulda converted correctly.");
        assertEquals("S3IdProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterUppercase( "s3-id-prop" ), "Shoulda converted correctly.");
        assertEquals("S3IdProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterUppercase( "S3IdProp" ), "Shoulda converted correctly.");
        assertEquals("SomeProp", NamingConvention.toCamelCaseNamingConventionWithFirstLetterUppercase( "someProp" ), "Shoulda converted correctly.");
    }
    

    @Test
    public void testToUnderscoreCaseNamingConventionNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                NamingConvention.toUnderscoredNamingConvention( null );
            }
        } );
    }
    
    
    @Test
    public void testToUnderscoreCaseNamingConventionDoesSo()
    {
        assertEquals("name_prop", NamingConvention.toUnderscoredNamingConvention( "NAME_PROP" ), "Shoulda converted correctly.");
        assertEquals("name_prop", NamingConvention.toUnderscoredNamingConvention( "NameProp" ), "Shoulda converted correctly.");
        assertEquals("s3_id_prop", NamingConvention.toUnderscoredNamingConvention( "s3IdProp" ), "Shoulda converted correctly.");
        assertEquals("name_prop", NamingConvention.toUnderscoredNamingConvention( "name_prop" ), "Shoulda converted correctly.");
        assertEquals("name_prop", NamingConvention.toUnderscoredNamingConvention( "name-prop" ), "Shoulda converted correctly.");
    }
}
