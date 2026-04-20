/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import java.util.Set;

import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface TeacherService 
    extends BeansRetriever< Teacher >, BeanCreator< Teacher >, BeanUpdater< Teacher >, BeanDeleter
{
    void create( final Set< Teacher > teachers );

    
    public int removeExistentPersistedBeansFromSetOfTeachers( final Set< Teacher > beans );
    

    int getInitializerCallCount();


    void addAnotherInitializer();
}
