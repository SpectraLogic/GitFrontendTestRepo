/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.replicationtarget;

import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.PublicCloudDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class TargetInitializationUtil
{
    private TargetInitializationUtil()
    {
        // singleton
    }
    
    
    public static TargetInitializationUtil getInstance()
    {
        return INSTANCE;
    }
    
    
    public void prepareForPutReplication(
            final BeansServiceManager serviceManager,
            final Ds3Target target, 
            final Job job, 
            final boolean force,
            final Ds3Connection connection )
    {
        synchronized ( m_initTargetLock )
        {
            prepareForPutReplicationInternal( serviceManager, target, job, force, connection );
        }
    }
    
    
    private void prepareForPutReplicationInternal(
            final BeansServiceManager serviceManager,
            final Ds3Target target, 
            final Job job, 
            final boolean force,
            final Ds3Connection connection )
    {
        if ( job.isAggregating() )
        {
            LOG.info( "Will not initiate aggregating PUT job on target " + target.getId() + "." );
            return;
        }
        if ( connection.isJobExistant( job.getId() ) )
        {
            return;
        }
        
        final Bucket bucket = serviceManager.getRetriever( Bucket.class ).attain( job.getBucketId() );
        final Ds3DataReplicationRule rule = 
                serviceManager.getRetriever( Ds3DataReplicationRule.class ).attain( Require.all( 
                        Require.beanPropertyEquals( 
                                DataPlacement.DATA_POLICY_ID, bucket.getDataPolicyId() ),
                        Require.beanPropertyEquals( 
                                DataReplicationRule.TARGET_ID, target.getId() ) ) );
        if ( !connection.isBucketExistant( bucket.getName() ) )
        {
            connection.createBucket( bucket.getId(), bucket.getName(), rule.getTargetDataPolicy() );
        }
        if ( !force )
        {
            connection.verifySafeToCreatePutJob( bucket.getName() );
        }
        
        final JobReplicationSupport support = 
                new JobReplicationSupport( serviceManager, job.getId() );
        final DetailedJobToReplicate jtr = support.getJobToReplicate();
        connection.replicatePutJob( jtr, bucket.getName() );
    }
    
    
    public Set< UUID > getDs3TargetsToReplicateTo(
            final BeansRetrieverManager brm, 
            final UUID dataPolicyId )
    {
        final Set< Ds3DataReplicationRule > rules =
                brm.getRetriever( Ds3DataReplicationRule.class ).retrieveAll( 
                        Require.all(
                                Require.beanPropertyEquals(
                                        DataPlacement.DATA_POLICY_ID,
                                        dataPolicyId ),
                                Require.beanPropertyEquals( 
                                        DataReplicationRule.TYPE, 
                                        DataReplicationRuleType.PERMANENT ) ) ).toSet();
        
        return BeanUtils.extractPropertyValues( rules, DataReplicationRule.TARGET_ID );
    }
    
    
    public < R extends PublicCloudDataReplicationRule< R > > Set< UUID > getPublicCloudTargetsToReplicateTo(
            final Class< R > ruleType,
            final BeansRetrieverManager brm,
            final UUID dataPolicyId )
    {
        final Set< R > rules =
                brm.getRetriever( ruleType ).retrieveAll( 
                        Require.all( 
                                Require.beanPropertyEquals(
                                        DataPlacement.DATA_POLICY_ID,
                                        dataPolicyId ),
                                Require.beanPropertyEquals( 
                                        DataReplicationRule.TYPE, 
                                        DataReplicationRuleType.PERMANENT ) ) ).toSet();
        
        return BeanUtils.extractPropertyValues( rules, DataReplicationRule.TARGET_ID );
    }
    

    private final Object m_initTargetLock = new Object();
    private final static TargetInitializationUtil INSTANCE = new TargetInitializationUtil();
    private final static Logger LOG = Logger.getLogger( TargetInitializationUtil.class );
}
