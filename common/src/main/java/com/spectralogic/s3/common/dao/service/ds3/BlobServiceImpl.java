/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;

final class BlobServiceImpl extends BaseService< Blob > implements BlobService
{
    BlobServiceImpl()
    {
        super( Blob.class );
    }
    
    
    public long getSizeInBytes( final Set< UUID > blobIds )
    {
        return getDataManager().getSum(
                Blob.class,
                Blob.LENGTH,
                Require.beanPropertyEqualsOneOf( Identifiable.ID, blobIds ) );
    }
    
    
    public void delete( final Set< UUID > blobIds )
    {
        getDataManager().deleteBeans(
                Blob.class,
                Require.beanPropertyEqualsOneOf( Identifiable.ID, blobIds ) );
    }
}
