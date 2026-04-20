/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansRetrieverInitializer;

final class TeacherServiceImpl extends BaseService< Teacher > implements TeacherService
{
    protected TeacherServiceImpl()
    {
        super( Teacher.class );
        addInitializer( new BeansRetrieverInitializer()
        {
            public void initialize()
            {
                m_callCount.incrementAndGet();
            }
        } );
    }
    
    
    public int removeExistentPersistedBeansFromSetOfTeachers( final Set< Teacher > beans )
    {
        return super.removeExistentPersistedBeansFromSet( beans );
    }

    
    public int getInitializerCallCount()
    {
        return m_callCount.get();
    }


    public void addAnotherInitializer()
    {
        addInitializer( new BeansRetrieverInitializer()
        {
            public void initialize()
            {
                //empty
            }
        } );
    }
    
    
    private final AtomicInteger m_callCount = new AtomicInteger();
}
