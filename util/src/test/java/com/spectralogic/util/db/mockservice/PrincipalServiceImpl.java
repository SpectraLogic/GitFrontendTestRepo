/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockservice;

import com.spectralogic.util.db.mockdomain.Principal;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.iterate.PreProcessor;

final class PrincipalServiceImpl extends BaseService< Principal > implements PrincipalService
{
    PrincipalServiceImpl()
    {
        super( Principal.class, GenericFailure.INTERNAL_ERROR );
        
        addCustomBeanPopulationProcessor( new PreProcessor< Principal >()
        {
            public void process( final Principal value )
            {
                value.setName( "Prince-" + value.getName() + "-ipal" );
            }
        } );
    }
}
