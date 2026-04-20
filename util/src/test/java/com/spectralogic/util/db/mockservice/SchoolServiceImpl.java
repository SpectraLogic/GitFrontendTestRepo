/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import java.util.UUID;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.service.BaseDatabaseBeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;

class SchoolServiceImpl extends BaseDatabaseBeansRetriever< School > implements SchoolService
{
    SchoolServiceImpl()
    {
        super( School.class, GenericFailure.NOT_FOUND );
    }
    
    
    public void createSchool( final School school )
    {
        getDataManager().createBean( school );
    }
    
    
    public void deleteSchool( final UUID schoolId )
    {
        getDataManager().deleteBean( getServicedType(), schoolId );
    }
    
    
    public void updateSchoolAddress( final UUID schoolId, final String address )
    {
        getDataManager().updateBean(
                CollectionFactory.toSet( School.ADDRESS ),
                BeanFactory.newBean( School.class ).setAddress( address ).setId( schoolId ) );
    }
}
