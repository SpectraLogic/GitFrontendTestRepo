package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.marshal.XmlMarshaler;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class GetAbmConfigRequestHandler_Test  {
    @Test
    public void testAbmConfigRequestHandlerDoesNotBlowUp()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        DataPolicy dp = mockDaoDriver.createABMConfigDualCopyOnTape();
        mockDaoDriver.createDataPolicy("unused-policy");
        mockDaoDriver.createBucket(null, dp.getId(), "test-bucket1");
        mockDaoDriver.createBucket(null, dp.getId(), "test-bucket2");
        mockDaoDriver.createStorageDomain("unused-StorageDomain");
        final S3Target s3Target = mockDaoDriver.createS3Target("aws target");
        final Ds3Target ds3Target = mockDaoDriver.createDs3Target("ds3 target");
        mockDaoDriver.createDs3DataReplicationRule(dp.getId(), DataReplicationRuleType.PERMANENT, ds3Target.getId());
        mockDaoDriver.createS3DataReplicationRule(dp.getId(), DataReplicationRuleType.RETIRED, s3Target.getId());
        final Tape tape = mockDaoDriver.createTape();
        final Pool pool = mockDaoDriver.createPool();
        final PoolPartition poolPartition = mockDaoDriver.createPoolPartition(PoolType.ONLINE, "p1");
        final TapePartition tapePartition = mockDaoDriver.attainOneAndOnly(TapePartition.class);
        mockDaoDriver.updateBean(pool.setPartitionId(poolPartition.getId()), Pool.PARTITION_ID);
        mockDaoDriver.updateBean(tape.setPartitionId(tapePartition.getId()), Tape.PARTITION_ID);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/" + RestDomainType.ABM_CONFIG );
        driver.run();
        System.out.println(XmlMarshaler.formatPretty(driver.getResponseToClientAsString()));
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains(dp.getName());
        driver.assertResponseToClientContains("test-bucket1");
        driver.assertResponseToClientContains("test-bucket2");
        driver.assertResponseToClientContains("aws target");
        driver.assertResponseToClientContains("ds3 target");
        driver.assertResponseToClientDoesNotContain("unused-policy");
        driver.assertResponseToClientDoesNotContain("unused-StorageDomain");
    }
}
