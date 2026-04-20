/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import com.spectralogic.s3.common.dao.domain.ds3.FeatureKeyType;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.shared.ImportDirectiveService;
import com.spectralogic.s3.common.dao.service.target.TargetFailureService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.target.api.PublicCloudTargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.task.BaseImportTargetTask;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

abstract class BasePublicCloudTargetBlobStore
    < T extends DatabasePersistable & PublicCloudReplicationTarget< T >, 
      CF extends PublicCloudConnectionFactory< ?, T >, 
      FS extends TargetFailureService< ? >,
      ID extends ImportPublicCloudTargetDirective< ID > & DatabasePersistable >
    extends BaseTargetBlobStore< T, CF, FS > implements PublicCloudTargetBlobStore< ID >
{
    protected BasePublicCloudTargetBlobStore(
            final Class< T > targetType,
            final Class< FS > failureServiceType,
            final Class< ? extends ImportDirectiveService< ID > > importDirectiveServiceType,
            final CF connectionFactory,
            final FeatureKeyType featureKeyRequirementForWrites,
            final SystemFailureType featureKeyRestrictionFailure,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager )
    {
        super( targetType, 
               failureServiceType, 
               connectionFactory, 
               featureKeyRequirementForWrites, 
               featureKeyRestrictionFailure, 
               diskManager,
               jobProgressManager, 
               serviceManager );
        
        m_importDirectiveServiceType = importDirectiveServiceType;
        Validations.verifyNotNull( "Import directive service type", m_importDirectiveServiceType );
    }
    
    
    synchronized public void importTarget( final ID directive )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( m_importDirectiveServiceType ).deleteByEntityToImport( 
                    directive.getTargetId() );
            transaction.getService( m_importDirectiveServiceType ).create( directive );
            addImportTask( directive.getTargetId(), createImportTask( directive ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    

    protected abstract BaseImportTargetTask< T, ?, ID, CF, ? > createImportTask( final ID directive );
    
    
    private final Class< ? extends ImportDirectiveService< ID > > m_importDirectiveServiceType;
}
