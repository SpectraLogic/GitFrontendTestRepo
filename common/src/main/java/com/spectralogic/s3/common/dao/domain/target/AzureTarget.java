/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

@UniqueIndexes(
{
    @Unique( AzureTarget.ACCOUNT_NAME )
})
@Indexes( @Index( AzureTarget.NAME ) )
public interface AzureTarget 
    extends DataAlwaysOnlinePublicCloudReplicationTarget< AzureTarget >, DatabasePersistable
{
    String ACCOUNT_NAME = "accountName";
    
    String getAccountName();
    
    AzureTarget setAccountName( final String value );
    
    
    String ACCOUNT_KEY = "accountKey";
    
    @Secret
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    String getAccountKey();
    
    AzureTarget setAccountKey( final String value );
}
