/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.predicate.TruePredicate;


public class MockBeansServiceManager_Test 
{
    @Test
    public void testGetRetrieverReturnsNotNull()
    {
        final BeansRetriever< TestBean > retriever = new MockBeansServiceManager()
            .getRetriever( TestBean.class );
        assertNotNull(
                retriever,
                "Shoulda returned a non-null retriever." );
    }
    
    
    @Test
    public void testGetServiceReturnsNotNull()
    {
        final BeansRetriever< ? > service = new MockBeansServiceManager()
            .getService( BeansRetriever.class );
        assertNotNull(
                service,
                "Shoulda returned a non-null service." );
    }
    
    
    @Test
    public void testGetServicesReturnsEmpty()
    {
        final Set< BeansRetriever< ? >> services = new MockBeansServiceManager()
            .getServices( new TruePredicate< Class< ? > >() );
        assertNotNull(
                services,
                "Shoulda returned a non-null set of services."  );
        assertTrue(
                services.isEmpty(),
                "Shoulda returned an empty set of services."  );
    }
    
    
    @Test
    public void testStartTransactionReturnsNotNull()
    {
        final BeansServiceManager transaction = new MockBeansServiceManager()
            .startTransaction();
        assertNotNull(
                transaction,
                "Shoulda returned a non-null transaction." );
    }
    
    
    @Test
    public void testCommitTransactionDoesNotBlowUp()
    {
        new MockBeansServiceManager().commitTransaction();
    }
    
    
    @Test
    public void testCloseTransactionDoesNotBlowUp()
    {
        new MockBeansServiceManager().closeTransaction();
    }
}
