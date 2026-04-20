package com.spectralogic.s3.common.rpc.dataplanner;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;

public interface CreateBucketResource
{
    @RpcMethodReturnType( UUID.class )
    RpcFuture<UUID> createBucket( final Bucket bucket);
}
