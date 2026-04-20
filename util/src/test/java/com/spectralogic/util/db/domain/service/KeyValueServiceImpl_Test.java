/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.domain.KeyValue;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class KeyValueServiceImpl_Test 
{
    @Test
    public void testGetStringThrowsExceptionWhenNoKeyExists()
    {
        runKeyMissingTest( new KeyValueShouldFailContainer()
        {
            public void runCodeThatFails( final KeyValueService keyValueService )
            {
                keyValueService.getString( TEST_KEY );
            }
        } );
    }
    
    
    @Test
    public void testGetBooleanThrowsExceptionWhenNoKeyExists()
    {
        runKeyMissingTest( new KeyValueShouldFailContainer()
        {
            public void runCodeThatFails( final KeyValueService keyValueService )
            {
                keyValueService.getBoolean( TEST_KEY );
            }
        } );
    }
    
    
    @Test
    public void testGetDoubleThrowsExceptionWhenNoKeyExists()
    {
        runKeyMissingTest( new KeyValueShouldFailContainer()
        {
            public void runCodeThatFails( final KeyValueService keyValueService )
            {
                keyValueService.getDouble( TEST_KEY );
            }
        } );
    }
    
    
    @Test
    public void testGetIntThrowsExceptionWhenNoKeyExists()
    {
        runKeyMissingTest( new KeyValueShouldFailContainer()
        {
            public void runCodeThatFails( final KeyValueService keyValueService )
            {
                keyValueService.getInt( TEST_KEY );
            }
        } );
    }
    
    
    @Test
    public void testGetLongThrowsExceptionWhenNoKeyExists()
    {
        runKeyMissingTest( new KeyValueShouldFailContainer()
        {
            public void runCodeThatFails( final KeyValueService keyValueService )
            {
                keyValueService.getLong( TEST_KEY );
            }
        } );
    }
    
    
    @Test
    public void testGetStringReturnsDefaultWhenNoKeyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }
        
        final String defaultValue = "Test Default Value";
        assertEquals(defaultValue, keyValueService.getString( TEST_KEY, defaultValue ), "Shoulda returned the default value.");
    }
    
    
    @Test
    public void testGetBooleanReturnsDefaultWhenNoKeyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }
        
        final boolean defaultValue = true;
        assertEquals(defaultValue, keyValueService.getBoolean( TEST_KEY, defaultValue ), "Shoulda returned the default value.");
    }
    
    
    @Test
    public void testGetDoubleReturnsDefaultWhenNoKeyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }
        
        final double defaultValue = 123.456;
        assertEquals(
                defaultValue,
                keyValueService.getDouble( TEST_KEY, defaultValue ),
                .0001,
                "Shoulda returned the default value."
                );
    }
    
    
    @Test
    public void testGetIntReturnsDefaultWhenNoKeyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }
        
        final int defaultValue = 123456;
        assertEquals(defaultValue,  keyValueService.getInt(TEST_KEY, defaultValue), "Shoulda returned the default value.");
    }
    
    
    @Test
    public void testGetLongReturnsDefaultWhenNoKeyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }
        
        final int defaultValue = 123456789;
        assertEquals(defaultValue,  keyValueService.getLong(TEST_KEY, defaultValue), "Shoulda returned the default value.");
    }

    
    @Test
    public void testGetStringReturnsExpectedValueWhenKeySet()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final String defaultValue = "The default value.";
        final String value = "My other value";
        
        keyValueService.set( TEST_KEY, value );
        assertEquals(value, keyValueService.getString( TEST_KEY, defaultValue ), "Shoulda returned the set value.");
        assertEquals(value, keyValueService.getString( TEST_KEY ), "Shoulda returned the set value.");
    }

    
    @Test
    public void testGetBooleanReturnsExpectedValueWhenKeySet()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final boolean defaultValue = true;
        final boolean value = false;
        
        keyValueService.set( TEST_KEY, value );
        assertEquals(value, keyValueService.getBoolean( TEST_KEY, defaultValue ), "Shoulda returned the set value.");
        assertEquals(value, keyValueService.getBoolean( TEST_KEY ), "Shoulda returned the set value.");
    }

    
    @Test
    public void testGetDoubleReturnsExpectedValueWhenKeySet()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final double defaultValue = 123.456789;
        final double value = 987.654321;
        
        keyValueService.set( TEST_KEY, value );
        assertEquals(
                value,
                keyValueService.getDouble( TEST_KEY, defaultValue ),
                .0000001,
                "Shoulda returned the set value."
                 );
        assertEquals(
                value,
                keyValueService.getDouble( TEST_KEY ),
                .0000001,
                "Shoulda returned the set value."
                );
    }

    
    @Test
    public void testGetIntReturnsExpectedValueWhenKeySet()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final int defaultValue = 123456789;
        final int value = 987654321;
        
        keyValueService.set( TEST_KEY, value );
        assertEquals(value,  keyValueService.getInt(TEST_KEY, defaultValue), "Shoulda returned the set value.");
        assertEquals(value,  keyValueService.getInt(TEST_KEY), "Shoulda returned the set value.");
    }

    
    @Test
    public void testGetLongReturnsExpectedValueWhenKeySet()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final long defaultValue = 123456789;
        final long value = 987654321;
        
        keyValueService.set( TEST_KEY, value );
        assertEquals(value,  keyValueService.getLong(TEST_KEY, defaultValue), "Shoulda returned the set value.");
        assertEquals(value,  keyValueService.getLong(TEST_KEY), "Shoulda returned the set value.");
    }

    
    @Test
    public void testSetCanUpdateStringWhenKeyAlreadyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final String defaultValue = "The default value.";
        final String value1 = "My first value";
        final String value2 = "My second value";

        keyValueService.set( TEST_KEY, value1 );
        keyValueService.set( TEST_KEY, value2 );
        assertEquals(value2, keyValueService.getString( TEST_KEY, defaultValue ), "Shoulda returned the second set value.");
        assertEquals(value2, keyValueService.getString( TEST_KEY ), "Shoulda returned the second set value.");
    }

    
    @Test
    public void testSetCanUpdateBooleanWhenKeyAlreadyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final boolean defaultValue = true;
        final boolean value1 = false;
        final boolean value2 = true;

        keyValueService.set( TEST_KEY, value1 );
        keyValueService.set( TEST_KEY, value2 );
        assertEquals(value2, keyValueService.getBoolean( TEST_KEY, defaultValue ), "Shoulda returned the second set value.");
        assertEquals(value2, keyValueService.getBoolean( TEST_KEY ), "Shoulda returned the second set value.");
    }

    
    @Test
    public void testSetCanUpdateDoubleWhenKeyAlreadyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final double defaultValue = 123.456789;
        final double value1 = 987.654321;
        final double value2 = 987564.321;

        keyValueService.set( TEST_KEY, value1 );
        keyValueService.set( TEST_KEY, value2 );
        assertEquals(
                value2,
                keyValueService.getDouble( TEST_KEY, defaultValue ),
                .0000001,
                "Shoulda returned the second set value."
                 );
        assertEquals(
                value2,
                keyValueService.getDouble( TEST_KEY ),
                .0000001,
                "Shoulda returned the second set value."
                );
    }

    
    @Test
    public void testSetCanUpdateIntWhenKeyAlreadyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final int defaultValue = 123456789;
        final int value1 = 987654321;
        final int value2 = 78945123;

        keyValueService.set( TEST_KEY, value1 );
        keyValueService.set( TEST_KEY, value2 );
        assertEquals(value2,  keyValueService.getInt(TEST_KEY, defaultValue), "Shoulda returned the second set value.");
        assertEquals(value2,  keyValueService.getInt(TEST_KEY), "Shoulda returned the second set value.");
    }

    
    @Test
    public void testSetCanUpdateLongWhenKeyAlreadyExists()
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }

        final long defaultValue = 123456789;
        final long value1 = 987654321;
        final long value2 = 78945123;

        keyValueService.set( TEST_KEY, value1 );
        keyValueService.set( TEST_KEY, value2 );
        assertEquals(value2,  keyValueService.getLong(TEST_KEY, defaultValue), "Shoulda returned the second set value.");
        assertEquals(value2,  keyValueService.getLong(TEST_KEY), "Shoulda returned the second set value.");
    }
    

    private void runKeyMissingTest( final KeyValueShouldFailContainer shouldFailContainer )
    {
        final KeyValueService keyValueService = buildKeyValueService();
        if ( null == keyValueService )
        {
            return;
        }
        
        TestUtil.assertThrows(
                "Shoulda thrown a dao exception.",
                DaoException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        shouldFailContainer.runCodeThatFails( keyValueService );
                    }
                } );
    }
    
    
    private interface KeyValueShouldFailContainer
    {
        public void runCodeThatFails( final KeyValueService keyValueService );
    }
    
    
    private KeyValueService buildKeyValueService()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( KeyValue.class, KeyValueService.class );
        
        return dbSupport.getServiceManager().getService( KeyValueService.class );
    }
    
    
    private static final String TEST_KEY = "test_key";
}
