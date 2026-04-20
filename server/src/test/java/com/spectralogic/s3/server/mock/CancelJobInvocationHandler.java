package com.spectralogic.s3.server.mock;

import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class CancelJobInvocationHandler implements InvocationHandler {
    public CancelJobInvocationHandler(final DatabaseSupport dbSupport) {
        m_service = dbSupport.getServiceManager().getService(JobService.class);
    }


    public void throwUponInvocation() {
        m_throws = true;
    }


    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args)
            throws Throwable {
        m_jobIds.add((UUID) args[1]);
        if (m_deleteJobUponCancel) {
            m_service.delete((UUID) args[1]);
        }

        if (m_throws) {
            throw new RpcProxyException(
                    "Oops",
                    BeanFactory.newBean(Failure.class).setHttpResponseCode(411));
        }

        return new RpcResponse<>(null);
    }


    public List<UUID> getJobIds() {
        return m_jobIds;
    }


    public void setDeleteJobUponCancel(final boolean value) {
        m_deleteJobUponCancel = value;
    }


    private volatile boolean m_throws;
    public volatile boolean m_deleteJobUponCancel;
    private final List<UUID> m_jobIds = new ArrayList<>();
    private final JobService m_service;
}//end inner class
