/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.lang.CollectionFactory;

final class FeatureKeyServiceImpl extends BaseService< FeatureKey > implements FeatureKeyService
{
    FeatureKeyServiceImpl()
    {
        super( FeatureKey.class );
    }

    
    @Override
    public void create( final FeatureKey fk )
    {
        create( CollectionFactory.toSet( fk ) );
    }


    @Override
    public void create( final Set< FeatureKey > fks )
    {
        verifyInsideTransaction();
        getDataManager().deleteBeans(
                getServicedType(),
                Require.beanPropertyEqualsOneOf(
                        FeatureKey.KEY, 
                        BeanUtils.extractPropertyValues( fks, FeatureKey.KEY ) ) );
        getDataManager().createBeans( fks );
    }
}
