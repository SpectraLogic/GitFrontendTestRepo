package com.spectralogic.s3.common.platform.spectrads3;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.rpc.dataplanner.JobResource;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.server.BaseQuiescableRpcResource;

public abstract class BaseQuiescableJobResource extends BaseQuiescableRpcResource implements JobResource
{
    public BaseQuiescableJobResource( final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_serviceManager = serviceManager;
    }


    @Override
    protected void forceQuiesceAndPrepareForShutdown()
    {
        cleanUpCompletedJobsAndJobChunks();
        for ( final Job j : m_serviceManager.getRetriever( Job.class ).retrieveAll().toSet() )
        {
            cancelJob( null, j.getId(), true );
        }
    }


    @Override
    protected String getCauseForNotQuiesced()
    {
        cleanUpCompletedJobsAndJobChunks();
        final int activeJobCount = m_serviceManager.getService( JobService.class ).getCount();
        if ( 0 < activeJobCount )
        {
            return activeJobCount + " active jobs in the system";
        }
        return null;
    }
    
    
    protected final BeansServiceManager m_serviceManager;
}
