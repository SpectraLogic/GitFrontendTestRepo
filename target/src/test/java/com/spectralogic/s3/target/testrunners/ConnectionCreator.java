package com.spectralogic.s3.target.testrunners;

import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;

public interface ConnectionCreator < T extends PublicCloudReplicationTarget< T >>
{
    PublicCloudConnection create();
}

