/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class DatabaseNamingConvention_Test 
{
    @Test
    public void testToBeanPropertyNameNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    DatabaseNamingConvention.toBeanPropertyName( null );
                }
            } );
    }
    
    
    @Test
    public void testToBeanPropertyNameDoesSo()
    {
        assertEquals("nameProp", DatabaseNamingConvention.toBeanPropertyName( "name_prop" ), "Shoulda converted correctly.");
        assertEquals("s3IdProp", DatabaseNamingConvention.toBeanPropertyName( "s3_id_prop" ), "Shoulda converted correctly.");
    }
    

    @Test
    public void testToDatabaseColumnNameNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                DatabaseNamingConvention.toDatabaseColumnName( null );
            }
            } );
    }
    
    
    @Test
    public void testToDatabaseColumnNameDoesSo()
    {
        assertEquals("name_prop", DatabaseNamingConvention.toDatabaseColumnName( "nameProp" ), "Shoulda converted correctly.");
        assertEquals("s3_id_prop", DatabaseNamingConvention.toDatabaseColumnName( "s3IdProp" ), "Shoulda converted correctly.");
    }
    

    @Test
    public void testToDatabaseEnumNameNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                DatabaseNamingConvention.toDatabaseEnumName( null );
            }
            } );
    }
    
    
    @Test
    public void testToDatabaseEnumNameDoesSo()
    {
        assertEquals("overriden.some_mock_type", DatabaseNamingConvention.toDatabaseEnumName( SomeMockType.class ), "Shoulda converted correctly.");
    }
    

    @Test
    public void testToDatabaseTableNameNullArgNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    DatabaseNamingConvention.toDatabaseTableName( null );
                }
            } );
    }
    
    
    @Test
    public void testToDatabaseTableNameDoesSo()
    {
        assertEquals("lang.correctly_defined_database_table_type", DatabaseNamingConvention.toDatabaseTableName( CorrectlyDefinedDatabaseTableType.class ), "Shoulda converted correctly.");
    }
    

    @Test
    public void testToJavaTypeNullPackageNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                DatabaseNamingConvention.toJavaType( 
                        (Class<?>)null, 
                        "some_mock_type" );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                DatabaseNamingConvention.toJavaType( 
                        (String)null, 
                        "some_mock_type" );
            }
        } );
    }
    

    @Test
    public void testToJavaTypeNullDatabaseTypeNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                DatabaseNamingConvention.toJavaType( 
                        this.getClass().getPackage().getName(), 
                        null );
            }
        } );
    }
    
    
    @Test
    public void testToJavaTypeDoesSoWhenTypeIsDefinedAtTopLevel()
    {
        assertEquals(AnotherMockType.class, DatabaseNamingConvention.toJavaType(
                        getClass().getPackage().getName(),
                        "another_mock_type" ), "Shoulda converted correctly.");
        assertEquals(AnotherMockType.class, DatabaseNamingConvention.toJavaType(
                        getClass().getPackage().getName(),
                        "public.another_mock_type" ), "Shoulda converted correctly.");
    }
    
    
    @Test
    public void testToJavaTypeDoesSoWhenTypeIsDefinedInsideAnotherClass()
    {
        assertEquals(SomeMockType.class, DatabaseNamingConvention.toJavaType(
                        DatabaseNamingConvention_Test.class,
                        "some_mock_type" ), "Shoulda converted correctly.");
        assertEquals(SomeMockType.class, DatabaseNamingConvention.toJavaType(
                        DatabaseNamingConvention_Test.class,
                        "public.some_mock_type" ), "Shoulda converted correctly.");
    }
    
    
    @Schema( "overriden" )
    public enum SomeMockType
    {
        // empty
    }
}
