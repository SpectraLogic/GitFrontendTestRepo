/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain.bean;

import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.service.BaseService;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.TestNotificationRegistration;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.notification.domain.NotificationPayloadGenerator;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class HttpNotificationEvent_Test 
{
    @Test
    public void testConstructorNullRetrieverNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new HttpNotificationEvent( 
                        null,
                        InterfaceProxyFactory.getProxy( NotificationPayloadGenerator.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullPayloadGeneratorNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new HttpNotificationEvent( 
                        new MockBeansServiceManager().getRetriever( TestNotificationRegistration.class ),
                        null );
            }
        } );
    }
    

    @Test
    public void testHappyConstruction()
    {
        final BaseService<TestNotificationRegistration> retriever = new BaseService<TestNotificationRegistration>(TestNotificationRegistration.class) {};
        retriever.setInitParams(
                new MockBeansServiceManager(),
                InterfaceProxyFactory.getProxy( DataManager.class, null ),
                new MockBeansServiceManager().getNotificationEventDispatcher()
        );
        new HttpNotificationEvent(
                retriever,
                InterfaceProxyFactory.getProxy( NotificationPayloadGenerator.class, null ) );
    }
}
