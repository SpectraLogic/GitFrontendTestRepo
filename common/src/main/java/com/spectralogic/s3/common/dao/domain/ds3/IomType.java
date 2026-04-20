package com.spectralogic.s3.common.dao.domain.ds3;


import com.spectralogic.util.marshal.CustomMarshaledEnumConstantName;

public enum IomType
{
    //Custom enum names are used for backward compatibility
    @CustomMarshaledEnumConstantName("NO")
    NONE,
    @CustomMarshaledEnumConstantName("YES")
    STAGE,
    @CustomMarshaledEnumConstantName("PERMANENT_ONLY")
    STANDARD_IOM
}
