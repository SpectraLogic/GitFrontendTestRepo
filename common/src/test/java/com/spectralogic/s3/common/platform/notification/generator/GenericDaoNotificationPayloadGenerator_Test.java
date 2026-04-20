/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.platform.notification.domain.payload.GenericDaoNotificationPayload;
import com.spectralogic.util.db.lang.SqlOperation;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class GenericDaoNotificationPayloadGenerator_Test 
{
    @Test
    public void testConstructorNullSqlOperationNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new GenericDaoNotificationPayloadGenerator(
                        null,
                        this.getClass(),
                        CollectionFactory.toSet( UUID.randomUUID() ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullDaoTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new GenericDaoNotificationPayloadGenerator(
                        SqlOperation.values()[ 0 ],
                        null,
                        CollectionFactory.toSet( UUID.randomUUID() ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullIdsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new GenericDaoNotificationPayloadGenerator(
                        SqlOperation.values()[ 0 ],
                        this.getClass(),
                        null );
            }
        } );
    }
    

    @Test
    public void testConstructorEmptyIdsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new GenericDaoNotificationPayloadGenerator(
                        SqlOperation.values()[ 0 ],
                        this.getClass(),
                        CollectionFactory.< UUID >toSet() );
            }
        } );
    }
    

    @Test
    public void testHappyConstruction()
    {
        new GenericDaoNotificationPayloadGenerator(
                SqlOperation.values()[ 0 ],
                this.getClass(),
                CollectionFactory.toSet( UUID.randomUUID() ) );
    }
    
    
    @Test
    public void testResponseIsCorrect()
    {
        final SqlOperation sqlOperation = SqlOperation.values()[ 0 ];
        final Class< ? > clazz = this.getClass();
        final UUID id = UUID.randomUUID();
        final NotificationPayloadGenerator generator = new GenericDaoNotificationPayloadGenerator(
                sqlOperation,
                clazz,
                CollectionFactory.toSet( id ) );
        final GenericDaoNotificationPayload event = 
                (GenericDaoNotificationPayload)generator.generateNotificationPayload();
        NotificationPayloadTracker.register( event );
        assertEquals(
                sqlOperation,
                event.getSqlOperation(),
                "Shoulda returned a sensible response."
                );
        assertEquals(
                clazz.getName(),
                event.getDaoType(),
                "Shoulda returned a sensible response."
                );
        assertEquals(
                1,
                event.getIds().length,
                "Shoulda returned a sensible response."
                );
        assertEquals(
                id,
                event.getIds()[ 0 ],
                "Shoulda returned a sensible response."
                 );
    }
}
