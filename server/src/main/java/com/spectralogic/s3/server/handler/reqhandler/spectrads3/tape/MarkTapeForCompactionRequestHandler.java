package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.composite.IomService;
import com.spectralogic.s3.common.dao.service.composite.IomServiceImpl;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;


public final class MarkTapeForCompactionRequestHandler extends BaseRequestHandler
{
    public MarkTapeForCompactionRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication.USER ),
                new RestfulCanHandleRequestDeterminer(
                        RestOperationType.MARK_FOR_COMPACTION,
                        RestDomainType.TAPE ) );
    }

    @Override
    protected ServletResponseStrategy handleRequestInternal(DS3Request request, CommandExecutionParams params) {
        final Tape tape = request.getRestRequest().getBean(
                params.getServiceManager().getRetriever( Tape.class ) );

        final IomService iomService = new IomServiceImpl( params.getServiceManager() );
        iomService.markSingleTapeForAutoCompaction( tape.getId() );

        return BeanServlet.serviceModify(
                params,
                params.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ) );
    }
}
