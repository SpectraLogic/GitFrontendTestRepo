package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.ds3.CloudNamingMode;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

@ExcludeDefaultsFromMarshaler
public interface TargetApiBean extends ReplicationTarget<TargetApiBean>, Identifiable {

    @MarshalXmlAsAttribute
    String getName();


    String CLOUD_NAMING_MODE = "cloudNamingMode";

    @Optional
    @ExcludeFromMarshaler(When.VALUE_IS_NULL)
    CloudNamingMode getCloudNamingMode();

    TargetApiBean setCloudNamingMode(final CloudNamingMode value );
}
