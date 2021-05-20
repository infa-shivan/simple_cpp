/*******************************************************************************
* ----- Persistent Systems Pvt. Ltd., Pune ----------- 
* Copyright (c) 2006
* All rights reserved.
* Name        : PCNReader_MetaExtension.hpp
* Description : 
*******************************************************************************/
#ifndef  __PCN_READER__METADATAEXT_
#define __PCN_READER__METADATAEXT_
#pragma once

#include "utils.hpp"

class NetMetaExtension
{
public:
    IUString m_sCustomQuery;
    IUString m_sSelectDistinct;
    IUString m_sFilterString;
    IINT32 m_nSortedPorts;
    IUString m_sJoinType;
    NetMetaExtension(void);
public:
    ~NetMetaExtension(void);
};
#endif
