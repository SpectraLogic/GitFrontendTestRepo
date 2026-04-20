/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import java.util.UUID;

import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface SchoolService extends BeansRetriever< School >
{
    public void createSchool( final School school );
    
    
    public void deleteSchool( final UUID schoolId );
    
    
    public void updateSchoolAddress( final UUID schoolId, final String address );
}
