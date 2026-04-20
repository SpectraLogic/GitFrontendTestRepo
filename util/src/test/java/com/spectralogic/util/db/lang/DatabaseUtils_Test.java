/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Priority;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;
import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import com.spectralogic.util.thread.wp.WorkPool;

public final class DatabaseUtils_Test 
{
    @Test
    public void testGetChecksumReturnsExpectedChecksum()
    {
        final String sql =
                "SELECT *" + Platform.NEWLINE
                + "-- test comment" + Platform.NEWLINE
                + Platform.NEWLINE
                + "FROM mytable;";
        assertEquals("ca0ec0ef8a8f3a6352f19247c503be62", DatabaseUtils.getChecksum( sql ), "Shoulda generated the same checksum as 'SELECT *FROM mytable;'.");
    }
    
    
    @Test
    public void testGetNextTransactionNumberProbablyAlwaysReturnsUniqueValues()
            throws InterruptedException, ExecutionException
    {
        final WorkPool workPool = SystemWorkPool.getInstance();
        final int concurrentUpdatesToAttempt = 10000;
        final Collection< Future< ? > > futures =
                new ArrayList<>( concurrentUpdatesToAttempt );
        final Set< Long > transactionNumbers =
                Collections.newSetFromMap( new ConcurrentHashMap< Long, Boolean >() );
        final Runnable getNextAction = new Runnable()
        {
            public void run()
            {
                transactionNumbers.add( Long.valueOf( DatabaseUtils.getNextTransactionNumber() ) );
            }
        };
        for ( int i = 0; i < concurrentUpdatesToAttempt; i++ )
        {
            futures.add( workPool.submit( getNextAction ) );
        }
        for ( final Future< ? > future : futures )
        {
            future.get();
        }
        assertEquals(concurrentUpdatesToAttempt,  transactionNumbers.size(), "Shoulda had the same number of transaction numbers as calls to the number generator.");
    }
    
    
    @Test
    public void testGetTransactionDescriptionReturnsExpectedDescription()
    {
        assertEquals("SQLTrans-123456789", DatabaseUtils.getTransactionDescription( Long.valueOf( 123456789L ) ), "Shoulda described the transaction correctly.");
    }
    
    
    @Test
    public void testGetTransactionDescriptionReturnsEmptyStringWhenNullProvided()
    {
        assertEquals("", DatabaseUtils.getTransactionDescription( null ), "Shoulda described a null transaction correctly.");
    }
    
    
    @Test
    public void testGetPrimaryKeyPropertyNameReturnsIdentifiableIdWhenDatabasePersistableProvided()
    {
        assertEquals(Identifiable.ID, DatabaseUtils.getPrimaryKeyPropertyName( County.class ), "Shoulda returned the id property name.");
    }
    
    
    @Test
    public void testGetPrimaryKeyPropertyNameWhenNonDatabasePersistableProvidedNotAllowed()
    {
        TestUtil.assertThrows(
                "Shoulda thrown an unsupported operation exception.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        DatabaseUtils.getPrimaryKeyPropertyName( Object.class );
                    }
                } );
    }
    
    
    @Test
    public void testGetPersistablePropertyNamesReturnsExpectedNames()
    {
        final Object actual = DatabaseUtils.getPersistablePropertyNames( BeanWithPropertyAnnotations.class );
        assertEquals(new HashSet<String>( Arrays.asList( Identifiable.ID, BeanWithPropertyAnnotations.COUNTY_ID ) ),
                actual,
                "Shoulda returned the expected property names.");
    }

    
    @Test
    public void testIsPersistableToDatabaseReturnsFalseWhenNotDatabasePersistable()
    {
        assertFalse(
                DatabaseUtils.isPersistableToDatabase( Object.class, "test_prop" ),
                "Shoulda not been persistable.");
    }
    
    
    @Test
    public void testIsPersistableToDatabaseReturnsFalseWhenFieldExcludedOnSetter()
    {
        assertFalse(
                DatabaseUtils.isPersistableToDatabase(
                                BeanWithPropertyAnnotations.class,
                                BeanWithPropertyAnnotations.ALTERNATE_NAME ),
                "Shoulda not been persistable.");
    }
    
    
    @Test
    public void testIsPersistableToDatabaseReturnsFalseWhenFieldExcludedOnGetter()
    {
        assertFalse(
                DatabaseUtils.isPersistableToDatabase(
                                BeanWithPropertyAnnotations.class,
                                BeanWithPropertyAnnotations.NAME ),
                "Shoulda not been persistable.");
    }
    
    
    @Test
    public void testIsPersistableToDatabaseReturnsTrueWhenSetterIsMissing()
    {
        TestUtil.assertThrows(
                "Shoulda thrown an illegal arugment exception.",
                IllegalArgumentException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        DatabaseUtils.isPersistableToDatabase(
                                BeanWithMissingSettersAndGetters.class,
                                BeanWithMissingSettersAndGetters.SETTER_MISSING );
                    }
                } );
    }
    
    
    @Test
    public void testIsPersistableToDatabaseReturnsTrueWhenGetterIsMissing()
    {
        TestUtil.assertThrows(
                "Shoulda thrown an illegal arugment exception.",
                IllegalArgumentException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        DatabaseUtils.isPersistableToDatabase(
                                BeanWithMissingSettersAndGetters.class,
                                BeanWithMissingSettersAndGetters.GETTER_MISSING );
                    }
                } );
    }
    
    
    @Test
    public void testIsPersistableToDatabaseReturnsTrueWhenFieldIsNormal()
    {
        assertTrue(
                DatabaseUtils.isPersistableToDatabase(
                                BeanWithPropertyAnnotations.class,
                                BeanWithPropertyAnnotations.COUNTY_ID ),
                "Shoulda been persistable.");
    }
    
    
    @Test
    public void testToDatabaseTypeWhenSupportedTypeProvided()
    {
        assertEquals("varchar", DatabaseUtils.toDatabaseType( String.class, false), "Shoulda returned the correct database String type.");
        assertEquals("integer", DatabaseUtils.toDatabaseType( Integer.class, false), "Shoulda returned the correct database Integer type.");
        assertEquals("bigint", DatabaseUtils.toDatabaseType( Long.class, false), "Shoulda returned the correct database Long type.");
        assertEquals("double precision", DatabaseUtils.toDatabaseType( Double.class, false), "Shoulda returned the correct database Double type.");
        assertEquals("uuid", DatabaseUtils.toDatabaseType( UUID.class, false), "Shoulda returned the correct database UUID type.");
        assertEquals("timestamp without time zone", DatabaseUtils.toDatabaseType( Date.class, false), "Shoulda returned the correct database Date type.");
        assertEquals("boolean", DatabaseUtils.toDatabaseType( Boolean.class, false), "Shoulda returned the correct database Boolean type.");
        assertEquals("lang.my_enum", DatabaseUtils.toDatabaseType( MyEnum.class, false), "Shoulda returned the correct database MyEnum type.");
    }
    
    
    @Test
    public void testToDatabaseTypeWhenUnSupportedTypeProvided()
    {
        TestUtil.assertThrows(
                "Shoulda thrown an unsupported operation exception since "
                        + "beans don't usually map directly to database types.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        DatabaseUtils.toDatabaseType( BeanWithPropertyAnnotations.class, false);
                    }
                } );
    }
    

    @Test
    public void testGetLogLevelWhenClassAnnotatedWithDebugProvided()
    {
        assertEquals(Priority.DEBUG_INT,  DatabaseUtils.getLogLevel(
                County.class, SqlOperation.INSERT, new TransactionLogLevelImpl()), "Shoulda returned the debug log level for INSERT.");
        assertEquals(Priority.DEBUG_INT,  DatabaseUtils.getLogLevel(
                County.class, SqlOperation.SELECT, new TransactionLogLevelImpl()), "Shoulda returned the debug log level for SELECT.");
        assertEquals(Priority.DEBUG_INT,  DatabaseUtils.getLogLevel(
                County.class, SqlOperation.UPDATE, new TransactionLogLevelImpl()), "Shoulda returned the debug log level for UPDATE.");
        assertEquals(Priority.DEBUG_INT,  DatabaseUtils.getLogLevel(
                County.class, SqlOperation.DELETE, new TransactionLogLevelImpl()), "Shoulda returned the debug log level for DELETE.");
    }
    

    @Test
    public void testGetLogLevelWhenClassWithNoAnnotatedProvided()
    {
        assertEquals(Priority.INFO_INT,  DatabaseUtils.getLogLevel(
                BeanWithPropertyAnnotations.class,
                SqlOperation.INSERT,
                new TransactionLogLevelImpl()), "Shoulda returned the info log level for INSERT.");
        assertEquals(Priority.DEBUG_INT,  DatabaseUtils.getLogLevel(
                BeanWithPropertyAnnotations.class,
                SqlOperation.SELECT,
                new TransactionLogLevelImpl()), "Shoulda returned the debug log level for SELECT.");
        assertEquals(Priority.INFO_INT,  DatabaseUtils.getLogLevel(
                BeanWithPropertyAnnotations.class,
                SqlOperation.UPDATE,
                new TransactionLogLevelImpl()), "Shoulda returned the info log level for UPDATE.");
        assertEquals(Priority.INFO_INT,  DatabaseUtils.getLogLevel(
                BeanWithPropertyAnnotations.class,
                SqlOperation.DELETE,
                new TransactionLogLevelImpl()), "Shoulda returned the info log level for DELETE.");
    }
    
    
    private enum MyEnum
    {
        VALUE1
    }
    
    
    private interface BeanWithMissingSettersAndGetters extends DatabasePersistable
    {
        String GETTER_MISSING = "getterMissing";
        
        void setGetterMissing( final String value);
        
        
        String SETTER_MISSING = "setterMissing";
        
        String getSetterMissing();
    }
        
        
    private interface BeanWithPropertyAnnotations extends DatabasePersistable
    {
        String NAME = "name";
        
        @ExcludeFromDatabasePersistence
        String getName();
        
        void setName( final String value);
        
        
        String ALTERNATE_NAME = "alternateName";
        
        String getAlternateName();

        @ExcludeFromDatabasePersistence
        void setAlternateName( final String value);
        

        String COUNTY_ID = "countyId";
        
        UUID getCountyId();
        
        void setCountyId( final UUID value);
    }
}
